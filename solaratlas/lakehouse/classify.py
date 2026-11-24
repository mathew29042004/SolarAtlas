"""Building-type classifier: residential / commercial / industrial / institutional.

Only ~0.6% of Indian footprints carry a type tag (from OSM). We train a Spark ML random forest
on those, using shape, density, nearby places and land use, then label every other building.
Tagged buildings keep their tag (type_source = 'osm'); the rest get the model's class and its
probability (type_source = 'model').

Honest caveats, also stored with the metrics: the tagged sample is biased toward mapped city
areas, and residential dominates, so classes are reweighted by inverse frequency and quality is
reported as macro-F1 and per-class recall on a held-out 20%, not just accuracy.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from solaratlas import config
from solaratlas.lakehouse.delta_utils import upsert_changed
from solaratlas.lakehouse.spark import get

NUMERIC = ["log_area", "perim_ratio", "compactness", "elongation", "n_vertices", "built_frac_ring",
           "ring_n", "r9_buildings", "r9_mean_area",
           "poi11_commercial", "poi11_industrial", "poi11_institutional",
           "poi10r_commercial", "poi10r_industrial", "poi10r_institutional"]
MIN_LABELS = 200


def model_dir(region_key: str):
    return config.DATA / "models" / f"building_type_{region_key}"


def train_and_predict(region_key: str, run_id: str) -> dict:
    spark = get()
    f = (spark.read.format("delta").load(config.table_path("silver", "building_features"))
         .where(F.col("region") == region_key).na.fill(0, NUMERIC)
         .na.fill({"compactness": 0.5, "elongation": 1.0}))
    labelled = f.where(F.col("osm_type").isNotNull())
    counts = {r.osm_type: r["count"] for r in labelled.groupBy("osm_type").count().collect()}
    n_lab = sum(counts.values())
    if n_lab < MIN_LABELS or len(counts) < 2:
        print(f"[classify] only {n_lab} labels; falling back to rules")
        return _rules(f, region_key, run_id, counts)

    total = sum(counts.values())
    weights = F.create_map(*[x for k, v in counts.items() for x in (F.lit(k), F.lit(total / (len(counts) * v)))])
    data = labelled.withColumn("w", weights[F.col("osm_type")])
    train, test = data.randomSplit([0.8, 0.2], seed=42)

    label_ix = StringIndexer(inputCol="osm_type", outputCol="label").fit(data)
    stages = [
        StringIndexer(inputCol="landuse_zone", outputCol="lz_ix", handleInvalid="keep"),
        OneHotEncoder(inputCols=["lz_ix"], outputCols=["lz_vec"]),
        VectorAssembler(inputCols=NUMERIC + ["lz_vec"], outputCol="features"),
        label_ix,
        RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="w",
                               numTrees=80, maxDepth=12, minInstancesPerNode=3, seed=42),
        IndexToString(inputCol="prediction", outputCol="predicted_type", labels=label_ix.labels),
    ]
    model = Pipeline(stages=stages).fit(train)
    pred = model.transform(test).cache()
    ev = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    metrics = {
        "region": region_key, "run_id": run_id, "trained_at": datetime.now(UTC).isoformat(),
        "labels": counts, "train_rows": train.count(), "test_rows": pred.count(),
        "accuracy": round(ev.evaluate(pred, {ev.metricName: "accuracy"}), 4),
        "weighted_f1": round(ev.evaluate(pred, {ev.metricName: "f1"}), 4),
    }
    per_class, f1s = {}, []
    for i, name in enumerate(label_ix.labels):
        p = ev.evaluate(pred, {ev.metricName: "precisionByLabel", ev.metricLabel: float(i)})
        r = ev.evaluate(pred, {ev.metricName: "recallByLabel", ev.metricLabel: float(i)})
        f1 = 2 * p * r / (p + r) if p + r else 0.0
        f1s.append(f1)
        per_class[name] = {"precision": round(p, 3), "recall": round(r, 3), "f1": round(f1, 3)}
    metrics["macro_f1"] = round(sum(f1s) / len(f1s), 4)
    metrics["per_class"] = per_class
    # the deployed rule (confidence threshold for non-residential), measured on the same held-out set
    thr = float(config.load("assumptions")["classifier"]["nonresidential_min_prob"])
    dep = pred.withColumn("prob", F.array_max(vector_to_array("probability"))) \
        .withColumn("final", F.when((F.col("predicted_type") != "residential") & (F.col("prob") < thr),
                                    "residential").otherwise(F.col("predicted_type")))
    tot = dep.count()
    deployed = {"threshold": thr,
                "accuracy": round(dep.where(F.col("final") == F.col("osm_type")).count() / tot, 4),
                "per_class": {}}
    for name in label_ix.labels:
        tp = dep.where((F.col("final") == name) & (F.col("osm_type") == name)).count()
        pp = dep.where(F.col("final") == name).count()
        ap = dep.where(F.col("osm_type") == name).count()
        deployed["per_class"][name] = {"precision": round(tp / pp, 3) if pp else None,
                                       "recall": round(tp / ap, 3) if ap else None}
    metrics["deployed"] = deployed
    cm = pred.groupBy("osm_type", "predicted_type").count().collect()
    metrics["confusion"] = [[r.osm_type, r.predicted_type, r["count"]] for r in cm]
    rf = model.stages[4]
    names = NUMERIC + [f"landuse={v}" for v in model.stages[0].labels]
    imp = sorted(zip(names, rf.featureImportances.toArray().tolist(), strict=False),
                 key=lambda t: -t[1])[:10]
    metrics["top_features"] = [[n, round(v, 4)] for n, v in imp]
    pred.unpersist()
    model.write().overwrite().save(str(model_dir(region_key)))
    _save_metrics(metrics)
    print(f"[classify] acc={metrics['accuracy']} macroF1={metrics['macro_f1']} labels={counts}")

    scored = (PipelineModel.load(str(model_dir(region_key))).transform(f)
              .withColumn("prob", F.array_max(vector_to_array("probability")))
              .withColumn("predicted_type",
                          F.when((F.col("predicted_type") != "residential") & (F.col("prob") < thr),
                                 "residential").otherwise(F.col("predicted_type"))))
    out = scored.select(
        "id", "region",
        F.coalesce("osm_type", "predicted_type").alias("building_type"),
        F.when(F.col("osm_type").isNotNull(), "osm").otherwise("model").alias("type_source"),
        F.when(F.col("osm_type").isNotNull(), F.lit(1.0)).otherwise(F.round("prob", 3)).alias("type_prob"),
        F.lit(run_id).alias("_run_id"))
    _write(out, region_key)
    metrics["predicted"] = {r.building_type: r["count"] for r in
                            spark.read.format("delta").load(config.table_path("silver", "building_type"))
                            .where(F.col("region") == region_key).groupBy("building_type").count().collect()}
    return metrics


def _rules(f, region_key, run_id, counts):
    t = (F.when(F.col("osm_type").isNotNull(), F.col("osm_type"))
         .when((F.col("landuse_class") == "industrial") | (F.col("poi11_industrial") > 0), "industrial")
         .when(F.col("landuse_zone").isin("education", "medical", "religious")
               | (F.col("poi11_institutional") > 0), "institutional")
         .when((F.col("landuse_class").isin("commercial", "retail")) | (F.col("poi11_commercial") > 1),
               "commercial")
         .otherwise("residential"))
    out = f.select("id", "region", t.alias("building_type"),
                   F.when(F.col("osm_type").isNotNull(), "osm").otherwise("rule").alias("type_source"),
                   F.lit(None).cast("double").alias("type_prob"), F.lit(run_id).alias("_run_id"))
    _write(out, region_key)
    m = {"region": region_key, "run_id": run_id, "method": "rules", "labels": counts,
         "trained_at": datetime.now(UTC).isoformat()}
    _save_metrics(m)
    return m


def _write(df, region_key):
    """Change-aware: a retrained model only 'changes' buildings whose label or probability moved."""
    # Compare the label only: probabilities drift slightly on every retrain, and comparing them
    # made all 25M rows "change". type_prob is refreshed whenever the label changes.
    upsert_changed(df, config.table_path("silver", "building_type"), region_key,
                   ["building_type", "type_source"])


def _save_metrics(m: dict):
    get().createDataFrame([(m["region"], m["run_id"], m["trained_at"], json.dumps(m))],
                          "region string, run_id string, trained_at string, metrics string") \
        .write.format("delta").mode("append").save(config.table_path("ops", "model_metrics"))
