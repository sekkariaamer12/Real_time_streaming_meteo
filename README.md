# Real_time_streaming_meteo
Ce projet vise à extraire des données météorologiques à partir d'une API afin d'analyser les conditions météorologiques et de mettre en place un modèle de prédiction météo. Pour ce projet, la ville de 'Rabat', capitale du Maroc, a été sélectionnée.

Les outils utilisés sont : Apache Kafka et Apache Airflow. Sqlite a été choisi comme base de données pour stocker les données récupérées par le consommateur d'Apache Kafka.

# Apache Kafka :
Apache Kafka est une plateforme de streaming distribuée conçue pour gérer efficacement les flux de données en temps réel à grande échelle. Il est souvent utilisé pour la diffusion de messages en temps réel, le traitement de flux, la gestion des événements, et bien plus encore. Kafka offre une architecture distribuée, une haute disponibilité, une faible latence et une scalabilité horizontale, ce qui en fait un choix populaire pour les applications nécessitant une gestion robuste des données en temps réel. Il repose sur le principe de journalisation distribuée, où les messages sont stockés de manière persistante sur un cluster de serveurs appelés "brokers".

# Apache Airflow :
Apache Airflow est une plateforme open-source de gestion de flux de travail (workflow) et d'ordonnancement de tâches. Il permet aux développeurs de définir, planifier et surveiller des workflows complexes sous forme de DAGs (Directed Acyclic Graphs). Chaque DAG représente un ensemble de tâches interdépendantes, où chaque tâche peut être un script Python, une commande shell, une requête SQL, etc. Airflow offre des fonctionnalités avancées telles que la planification dynamique, la gestion des dépendances, la reprise sur panne, la surveillance des tâches, et l'extensibilité via des hooks et des opérateurs personnalisés. Il est largement utilisé pour l'automatisation des pipelines de données, le traitement ETL, le déploiement de modèles ML, et d'autres workflows de données complexes.
