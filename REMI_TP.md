# Remise — TP Scala & Spark  
**Master 2 Data / Big Data**  
**Analyse exploratoire et détection de fraude**

---

## TP1 — Programmation Fonctionnelle (Scala)

### Objectif
Calculer le montant total des transactions françaises valides effectuées par des utilisateurs actifs et majeurs, en ignorant toute donnée invalide ou incomplète.

### Implémentation
- **Modèles** : `User` et `Transaction`.
- **isEligibleUser** : utilisateur actif avec âge défini et `>= 18`, implémenté via `Option.exists`.
- **findUser** : recherche d’un utilisateur par identifiant avec `List.find`, retour `Option[User]`.
- **isValidTransaction** : transaction valide si `amount > 0`, `country == "FR"` et `status == "VALID"`.
- **Pipeline fonctionnel final** :
  - Filtrage des transactions valides.
  - Jointure fonctionnelle avec les utilisateurs via `flatMap` et `Option`.
  - Agrégation du montant total via `sum`.
- **Bonus** : calcul du montant moyen retourné sous forme `Option[Double]`.

### Contraintes respectées
- Aucun usage de `var`, `for`, `while` ou `null`.
- Utilisation exclusive de `map`, `filter`, `flatMap` et `Option`.
- Pipeline lisible et robuste face aux données incomplètes.

---

## TP2 — Spark : Analyse exploratoire et fraude

### PARTIE 1 — Prise en main des données
- Chargement des fichiers CSV et JSON avec schéma inféré.
- Affichage des schémas et des premières lignes.
- Analyse de volumétrie : transactions, clients, cartes, commerçants.
- Analyse de qualité :
  - valeurs manquantes,
  - montants négatifs ou nuls,
  - MCC manquants,
  - erreurs transactionnelles.

> **Remarque** : les montants sont stockés sous forme de chaînes de caractères avec le symbole `$` dans le dataset d’origine.  
> Les analyses conservent cette devise afin d’éviter toute conversion arbitraire.

---

## PARTIE 2 — Analyse des montants et du temps

### Analyse des montants
- Calcul du montant **moyen**, **médian**, **minimum** et **maximum**.
- Étude de la distribution par tranches :
  - `< 10 $`
  - `10 – 50 $`
  - `50 – 200 $`
  - `> 200 $`

**Interprétation métier :**  
Les montants élevés sont généralement moins fréquents mais présentent un risque accru, en particulier lorsqu’ils sont associés à des erreurs ou à des comportements inhabituels.

### Analyse temporelle
- Extraction de l’heure, du jour de la semaine et du mois à partir de la date.
- Calcul du nombre de transactions par heure et par jour.

**Observation :**  
Des pics d’activité à des heures atypiques peuvent indiquer des comportements automatisés ou potentiellement frauduleux.

---

## PARTIE 3 — Enrichissement métier

### Jointure avec les MCC
- Jointure entre `transactions_data` et `mcc_codes`.
- Ajout de la colonne `merchant_category`.
- Calcul du **Top 10 des catégories** par volume de transactions.
- Calcul du **montant moyen par catégorie**.

**Analyse risque :**  
Certaines catégories concentrent des montants plus élevés et/ou davantage d’erreurs, ce qui peut indiquer un niveau de risque supérieur.

### Analyse des erreurs
- Identification des types d’erreurs les plus fréquents.
- Calcul du **taux d’erreur par carte** et par **client**.

**Interprétation :**  
Une carte ou un client présentant un taux d’erreur élevé peut être considéré comme suspect (tentatives répétées, refus multiples, fraude potentielle).

---

## PARTIE 4 — Approche fraude (sans Machine Learning)

### Création d’indicateurs
Les indicateurs suivants ont été calculés **par carte et par jour** :
- `tx_count` : nombre de transactions
- `total_amount` : montant total
- `distinct_cities` : nombre de villes différentes
- `error_ratio` : ratio de transactions en erreur

### Détection de comportements suspects
Création du DataFrame **`suspicious_cards`** basé sur des règles simples :
- plus de **20 transactions par jour**, ou
- transactions dans plus de **3 villes différentes**, ou
- montant total journalier supérieur à **1000 $**, ou
- **taux d’erreur > 30 %**.

Ces règles permettent un premier filtrage des comportements anormaux avant une analyse plus avancée.

---

## PARTIE 5 — Synthèse finale

### Patterns principaux observés
- Forte asymétrie dans la distribution des montants.
- Concentration de l’activité à certaines heures ou certains jours.
- Certaines catégories MCC présentent un risque plus élevé.

### Indicateurs utiles pour un futur modèle
- Nombre de transactions par carte et par jour.
- Montant total journalier par carte.
- Nombre de villes utilisées par carte.
- Ratio d’erreurs par carte et par client.
- Catégorie de commerçant (MCC).

### Limites des données
- Présence de valeurs manquantes (dates, villes, MCC).
- Hétérogénéité des formats et des schémas.
- Approche actuelle basée sur des règles simples, pouvant générer des faux positifs.

**Conclusion :**  
Ce travail constitue une base solide pour une future approche de détection de fraude basée sur le Machine Learning supervisé, en s’appuyant sur les indicateurs explorés et enrichis lors de ce TP.
