# DCA — Suivi des Créances Clients

Application de suivi des créances clients pour Design Constructions & Associés.

## Fonctionnalités

- 📥 Import du FEC (Fichier d'Écritures Comptables) et extraction des comptes clients (411xxx)
- 📥 Import de l'export CRM (Chantiers) pour enrichir les créances avec les infos dossier
- 🔗 Mapping entre code comptable (CompAuxNum) et référence dossier CRM
- 📊 Tableau de bord des créances ouvertes avec filtres (commercial, agence, état)
- 📝 Notes et historique des relances par client
- 📤 Export Excel par commercial (pour actions de relance)
- 📤 Export Power BI (dataset enrichi pour la direction)

## Stack technique

- **Python** + **Streamlit** — interface web
- **Pandas** — traitement du FEC et du CRM
- **Google Sheets** — base de données partagée (via gspread)
- **OpenPyXL** — génération des exports Excel

## Lancement local

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Structure

```
.
├── app.py                  # Application Streamlit
├── requirements.txt        # Dépendances Python
├── lancer_app.bat          # Script de lancement Windows
└── data/                   # Base locale (non versionnée)
```

## Sécurité

⚠️ Les fichiers de données (FEC, exports CRM, base locale) sont exclus du versioning via `.gitignore`.  
Les credentials Google sont gérés via Streamlit secrets (non versionnés).
