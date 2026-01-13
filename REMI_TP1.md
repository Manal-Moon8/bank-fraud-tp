 # Rémi – TP Scala (TP1 + démarrage TP2)

## TP1 – Programmation Fonctionnelle (Scala)

### Objectif
Calculer le montant total des transactions françaises valides, réalisées par des utilisateurs actifs et majeurs, en ignorant les données invalides/incomplètes.

### Contraintes respectées
- Pas de `var`, `for`, `while`, ni `null`
- Usage de `map`, `filter`, `flatMap` et `Option`

### Démarche
1. Définition des modèles `User` et `Transaction`.
2. `isEligibleUser`: utilisateur actif et age défini `>= 18` via `Option.exists`.
3. `findUser`: recherche avec `List.find` (retour `Option[User]`).
4. `isValidTransaction`: filtre sur `amount > 0`, `country == "FR"`, `status == "VALID"`.
5. Pipeline final: filtrage des transactions + jointure fonctionnelle via `Option` + agrégation.

### Résultats (jeu d’exemple)
- Total éligible: 420.0
- Bonus: moyenne retournée en `Option[Double]`

---

## TP2 – Spark (Analyse exploratoire)

### Objectif (démarrage)
Charger les datasets CSV/JSON, inspecter schémas/échantillons, puis réaliser la volumétrie et un premier diagnostic de qualité des données.

### Réalisé
- Chargement des fichiers (CSV avec header + inferSchema, JSON)
- Affichage schema + 10 premières lignes
- Volumétrie (transactions, clients, cartes, commerçants selon colonnes disponibles)
- Qualité: valeurs manquantes par colonne, montants <= 0, MCC manquant, présence d'erreurs (si colonnes disponibles)

### Données
Les fichiers proviennent d’un dataset anonymisé (Kaggle) et peuvent être volumineux; ils ne sont pas inclus dans le dépôt Git pour éviter les limitations de taille.
