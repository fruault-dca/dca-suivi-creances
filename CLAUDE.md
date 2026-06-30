# Suivi des créances clients — DCA (Design Constructions & Associés)

Application Streamlit de suivi/recouvrement des créances clients. Mono-fichier
(`app.py`), base de données Google Sheets, hébergée sur Streamlit Community Cloud,
code sur GitHub (`fruault-dca/dca-suivi-creances`).

## Architecture

```
Poste local  --git push-->  GitHub  --auto-deploy ~1min-->  Streamlit Cloud
                                                                   |
                                                            Google Sheets (BDD)
```

- **app.py** : toute l'app (≈2700 lignes). Pas de découpage en modules.
- **Google Sheets** : base partagée, accès via `gspread` + service account.
- **Streamlit Cloud** : surveille `main`, redéploie à chaque push (~1 min).
- **GitHub Actions** (`.github/workflows/reminders.yml` + `scripts/send_reminders.py`) :
  rappels email lun & jeu 8h (cron `0 6 * * 1,4`), indépendant de l'app.

## Workflow de dev (à suivre à chaque modif)

1. Éditer `app.py` (éditions ciblées).
2. **Vérifier la syntaxe** : `python -c "import ast; ast.parse(open('app.py',encoding='utf-8').read()); print('OK')"`.
   Tester la logique isolément quand c'est calculatoire (dates, stades, FIFO).
3. `git add` → `git commit` (message **en français**, descriptif) → `git push origin main`.
4. Prévenir l'utilisateur : « push fait, attends ~1 min » + quoi vérifier.
- Un commit = une modif cohérente. Ne jamais committer `.streamlit/secrets.toml`.
- Environnement Windows (PowerShell + Bash dispo).

## Secrets (jamais dans Git)

- `.streamlit/secrets.toml` (gitignored) : `google.sheet_id` + `gcp_service_account`.
  Modèle : `.streamlit/secrets.toml.example`.
- Streamlit Cloud → Settings → Secrets (même contenu).
- GitHub Actions → Secrets : `GCP_SERVICE_ACCOUNT`, `SHEET_ID`, `SMTP_USER`,
  `SMTP_PASSWORD`, `APP_URL`.

## Données : feuilles Google Sheets (dict `HEADERS` dans app.py)

- **creances** : écritures FEC comptes 411xxx (clients) + 416xxx (douteux).
  Champs clés : `comp_aux_num`, `piece_ref`, `piece_date`, `ecriture_date`,
  `debit`, `credit`, `ecriture_let` (lettrage), `import_date`.
- **dossiers** : export CRM "Chantiers" (`ref_client`, `client`, `commercial`,
  `conducteur`, `etat`, `date_reception`, etc.).
- **mapping** : facture→dossier (`piece_ref`, `ref_client`, `comp_aux_num`,
  `date_facture`, `situation`). Niveau **facture** (pas client).
- **notes** : relances (`assigne_a` pour la page Accueil + emails).
- **contentieux** : dossiers en contentieux (`responsable`, provisions).
- **consignations** : montants consignés chez l'huissier (saisie manuelle).
- **resumes** : 1 résumé direction par client (`resume`, `action_resume`,
  `responsable_action`, `nature_creance`, `date_recouvrement`).
- **users** : auth (`email`, `nom_affichage`, `actif`, `password_hash`, `role`).

## 3 sources importées (page Import)

- **FEC** (txt tab, latin-1/cp1252) : écritures comptables, comptes 411/416.
- **CRM Chantiers** (xlsx, onglet "Liste complète", en-têtes ligne 2).
- **PROGEMI** (xlsx) : lie n° facture → n° dossier + date + situation (appel de fonds).

## Logique métier critique (⚠️ pièges)

- **Mapping au niveau facture** : `piece_ref → ref_client` (un client a plusieurs dossiers).
- **Normalisation `piece_ref`** : `22/1` == `22/0000001` (on enlève les zéros de tête
  par segment `/`). Indispensable pour matcher FEC ↔ PROGEMI.
- **Normalisation `ref_client`** : `549` == `00549` ; dossiers groupés CRM `830/831`
  (chaque sous-réf pointe vers le dossier groupé).
- **Rapprochement FIFO** (`load_creances_enrichies`) : les encaissements non lettrés
  sont imputés aux plus anciennes factures du client (les paiements n'ont pas la
  même `piece_ref` que la facture). Une facture soldée disparaît.
- **Cascade de date de facture** (`date_facture_eff`) : `date_facture` (PROGEMI/manuel)
  > `piece_date` (FEC) > `ecriture_date`. Après clôture comptable la `piece_date`
  FEC repasse au 01/01 → saisie manuelle possible (onglet Mapping).
- **Hors CRM** : `ref_client == '__HORS_CRM__'` (factures FC* sans dossier).
  Affichées "Hors CRM". Auto-classables par regex, ou manuellement.
- **Dates** : stockées **ISO** (`YYYY-MM-DD`) en interne (pour le tri) ;
  affichées/exportées **JJ/MM/AAAA** via `fr_date` / `to_date_obj` (objets date Excel).
- **Quota Google Sheets (429)** : `read_sheet` caché 600s ; `_with_retry`
  (backoff expo) sur tous les appels gspread. Idem dans le script de rappels.

## Appels de fonds & routage commerciaux/conducteurs

7 stades (col `situation` du PROGEMI), ordre fixe, libellés variables →
`stage_from_situation()` détecte par mots **distinctifs** (⚠️ pas "achèvement",
ambigu) :
1 permis/accord · 2 fondation · 3 mur/maçonnerie · 4 eau · 5 cloison/air ·
6 équipement · 7 réception.
Routage **par dossier** (stade max) : ≤ stade 2 → **export commerciaux** ;
≥ stade 3 (ou chantier livré) → **export conducteurs** (toutes les factures du
dossier basculent, y compris fondations).

## Pages & exports

- **Accueil** : tâches assignées à l'utilisateur connecté (par échéance).
- **Import** : FEC / CRM / Mapping (PROGEMI) / Contentieux / Consignations / Utilisateurs.
- **Créances** : synthèses Chantiers en cours / livrés / contentieux, jours de
  retard colorés (vert <7, orange 7-29, rouge ≥30), filtres commercial/conducteur/état.
- **Notes & Relances** : notes + résumé direction (1/client).
- **Export** : Commerciaux / Conducteurs / Power BI / Contentieux / Direction.
  L'export Direction a un onglet "Évolution du dû" (baseline figée 1 111 971,48 au
  22/05/2026, En cours = Total dû appli).

## Charte graphique DCA

Vert `#60A020`, bleu marine `#2C3E50`, fond `#F0F0F0`, police Segoe UI.
Définie dans `.streamlit/config.toml` + CSS injecté en tête d'`app.py`.
Exports Excel : en-têtes bleu marine, totaux verts, format `#,##0.00 €`.

## Auth

App publique sur Streamlit Cloud → écran de login interne (`show_login`),
mots de passe hashés PBKDF2 dans la feuille `users`. Pas de système de rôles
actif (colonne `role` présente mais non câblée — tous les users ont accès à tout).
Cookie "se souvenir de moi" (7 j, jeton HMAC signé via `streamlit-cookies-controller`) :
`try_cookie_login()` au démarrage reconnecte sans mot de passe. Clé de signature =
`secrets['auth']['cookie_secret']` si fournie, sinon dérivée de la clé du service account.
