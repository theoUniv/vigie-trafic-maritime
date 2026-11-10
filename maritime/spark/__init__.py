"""Agrégats temps réel avec Apache Spark Structured Streaming.

Un job PySpark lit le topic Kafka `ais.positions` et calcule, sur des fenêtres de 5 minutes :
  * le trafic par zone (grille) et par type de navire ;
  * le trafic par type de navire sur toute la zone couverte ;
  * la fréquentation de chaque port.
Les résultats sont écrits dans PostgreSQL (tables `spark_*`).

`config` et `sink` n'importent pas pyspark : ils sont testables sans Java.
"""
