Pour configurer efficacement **Claude Code** (ou tout autre assistant de codage) et lui donner le contexte exact du livre de Brian Buzzelli, vous devez lui fournir un cahier des charges d'architecture (le "Pipeline") et les spécifications algorithmiques détaillées (les "Dimensions"). 

Voici la synthèse exhaustive de tout ce que l'auteur a développé, structurée exactement comme un prompt de contexte architectural pour votre projet.

### 1. L'Architecture Globale du Pipeline MDM (Les 3 États)
Votre pipeline doit faire transiter les données à travers trois états stricts :
*   **Raw data (Données brutes) :** Les données telles qu'elles sont reçues du fournisseur, sans aucune altération.
*   **Staged data (Données en transit) :** C'est ici que votre code doit appliquer les validations de qualité (DQS) et la détection d'anomalies. C'est la zone de quarantaine et de remédiation.
*   **Mastered data (Données maîtresses) :** Les données qui ont passé avec succès les tests de qualité. Elles sont certifiées, approuvées et poussées vers une base de données de production (Data abstractions/API) pour être consommées.

### 2. La Logique de Classification et d'Impact (DQS)
Votre code ne doit pas utiliser de simples booléens (Vrai/Faux). Chaque point de donnée (datum) doit être classifié selon les Spécifications de Qualité des Données (DQS) en trois niveaux de tolérance :
*   **V (Valid) :** Dans la tolérance (Vert).
*   **S (Suspect) :** S'approche de la limite de tolérance. Nécessite une investigation mais ne bloque pas forcément le système (Jaune).
*   **IV (Invalid) :** Hors tolérance absolue. La donnée est bloquée (Rouge).

Il faut aussi coder l'impact métier si la donnée échoue :
*   **H (High) :** Impact critique (ex: amendes, erreurs de trading). Bloque le processus.
*   **M (Medium) :** Processus ralenti, nécessite une correction.
*   **L (Low) :** Anomalie mineure, le système peut fonctionner avec.

### 3. Les 8 Algorithmes de Validation (Les Dimensions de la Qualité)
Vous devez demander à Claude Code de créer 8 modules/fonctions de validation distincts. Voici les règles mathématiques et logiques exactes dictées par l'auteur :

**1. Completeness (Complétude)**
*   *Logique :* Vérifie l'absence de valeurs nulles ou de chaînes vides.
*   *Calcul :* Compter les valeurs nulles. 
*   *Statuts :* M (Mandatory - Obligatoire) ou O (Optional - Optionnel). Si Mandatory et nul = IV. Si Optional et nul = S.

**2. Timeliness (Actualité / Fraîcheur)**
*   *Logique :* Vérifie si la date de la donnée est dans une plage temporelle acceptable.
*   *Exemple de calcul :* `Différence en jours = Date du jour - Date de la donnée`. 
*   *Tolérances :* V si < 30 jours ; S si entre 30 et 90 jours ; IV si >= 90 jours.

**3. Accuracy (Précision de l'exactitude)**
*   *Logique 1 (Source d'autorité) :* Comparaison stricte avec un fichier de contrôle (ex: Le Ticker dans le fichier `A` correspond-il au Ticker officiel du NASDAQ `B` ?).
*   *Logique 2 (Triangulation) :* Validation indirecte par mathématiques. Ex: La somme de 10 achats et 2 ventes doit donner une position de 8.

**4. Precision (Précision numérique)**
*   *Logique :* Vérifier l'échelle (le nombre de décimales).
*   *Algorithme :* Compter les chiffres après la virgule. 
*   *Tolérances (Exemple des taux de change) :* V = 6 décimales ; IV < 6 décimales. Un prix manquant de décimales entraîne une perte financière par effet cumulé.

**5. Conformity (Conformité)**
*   *Logique :* Utilisation de Regex pour valider un format standard.
*   *Exemple :* Les codes pays ISO doivent faire exactement 2 ou 3 lettres, ou "Market Cap Scale" doit être uniquement 'B' (Billion) ou 'M' (Million).

**6. Congruence (Similarité et Outliers - Le plus technique)**
S'applique aux séries temporelles. Trois algorithmes à coder :
*   *Prior Value Comparison (Comparaison avec la veille) :*
    Formule : `Valeur absolue (Prix J - Prix J-1) / Moyenne(Prix J, Prix J-1) * 100`. Ex: V si < 10%, S si entre 10% et 20%, IV si > 20%.
*   *Comparison to Average (Comparaison à la moyenne) :* 
    Formule : `Valeur absolue (Prix - Moyenne historique) / Moyenne historique * 100`. Ex: V si < 2%, S si entre 2% et 3%, IV si > 3%.
*   *Z-Score (Le plus robuste) :* 
    Formule : `Z-score = (Prix actuel - Moyenne des 5 derniers jours) / Écart-type des 5 derniers jours`. Ex: V si Z-score <= 2 ; S si Z-score > 2 et < 4 ; IV si Z-score >= 4.

**7. Collection (Intégrité de l'ensemble)**
*   *Logique :* Un portefeuille ou un indice (ex: S&P 500) doit contenir toutes ses composantes.
*   *Algorithme de contrôle :* 
    1. Comparer le `Record Count` (Nombre de lignes) avec le nombre officiel. 
    2. Comparer la `Somme des Market Values` avec la valeur de contrôle officielle. Tolérance : V si différence < 3%, sinon IV.

**8. Cohesion (Cohésion / Clés relationnelles)**
*   *Logique :* Vérification de l'intégrité référentielle.
*   *Algorithme :* Vérifier si la combinaison des clés étrangères (ex: `Date + Ticker` dans un fichier de portefeuille) correspond exactement à la clé primaire de la table de référence (Security Master).

### 4. Stratégie de contrôle (Pre-use vs Reconciliation)
Vous devez explicitement dire à Claude Code d'implémenter ces validations comme des **contrôles primaires "pré-utilisation" (Pre-use data validations)**. L'auteur insiste lourdement : les contrôles doivent rejeter ou alerter sur la donnée *avant* qu'elle n'entre dans les systèmes de calcul. Ne codez pas de système de "réconciliation post-utilisation" (laisser la donnée passer puis vérifier après), qui est inefficace (l'auteur compare cela avec humour au fait d'analyser le fumier d'une vache pour savoir ce qu'elle a mangé au lieu de contrôler sa nourriture).

### 5. L'Output final : Les "Scorecards" (Tableaux de bord)
Le pipeline final doit générer des métriques pour chaque point de donnée (datum), créant ainsi un "Data Quality Scorecard" (Heatmap).
Claude devra générer un rapport (par exemple un DataFrame Pandas) où chaque valeur testée est étiquetée par une couleur : `Vert (V)`, `Jaune (S)`, ou `Rouge (IV)`. 