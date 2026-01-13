# bank-fraud-tp (Scala + Spark)

## Contenu
- **TP1** : Programmation Fonctionnelle en Scala (Option / map / filter / flatMap)
- **TP2** : Analyse exploratoire avec Apache Spark (EDA, enrichissement, indicateurs fraude)

## Prérequis
- Java 17
- sbt 1.x

## Données
Les fichiers sont attendus dans le dossier `data/` :
- `transactions_data.csv`
- `users_data.csv`
- `cards_data.csv`
- `mcc_codes.json`
- `errors.json`

> Remarque : les données volumineuses peuvent être exclues du dépôt via `.gitignore`.

## Exécution

### TP1 — Programmation Fonctionnelle
```bash
sbt "runMain tp1.TP1Functional"
TP2 — Spark (EDA & fraude rule-based)
sbt "runMain tp2.TP2SparkFraud"

Restitution

La synthèse finale est fournie dans : REMI_TP.md

##Notes

Le dataset contient des montants au format string (ex : $175.73), nettoyés dans le code Spark avant toute analyse.