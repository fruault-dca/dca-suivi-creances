"""
Suivi des Créances Clients - Application Streamlit
Backend : Google Sheets (base de données partagée)
"""
import streamlit as st
import pandas as pd
import io
from datetime import datetime
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import time, random

st.set_page_config(page_title="Suivi Créances Clients", page_icon="📊", layout="wide")

# ============================================================
# CONFIGURATION GOOGLE SHEETS
# ============================================================
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

HEADERS = {
    'creances': ['id', 'comp_aux_num', 'comp_aux_lib', 'piece_ref', 'piece_date',
                 'ecriture_date', 'journal_code', 'ecriture_lib', 'debit', 'credit',
                 'ecriture_let', 'import_date'],
    'dossiers': ['ref_client', 'code_affaire', 'client', 'email1', 'email2',
                 'type_projet', 'adresse', 'cp', 'ville', 'constructeur',
                 'agence', 'commercial', 'conducteur', 'etat', 'stade', 'type_contrat',
                 'contrat_ht', 'contrat_ttc', 'contrat_rev_ht', 'contrat_rev_ttc',
                 'avenants_ht', 'avenants_ttc', 'date_signature', 'date_reception'],
    'mapping': ['piece_ref', 'ref_client', 'comp_aux_num', 'date_facture'],
    'notes': ['id', 'ref_client', 'comp_aux_num', 'date_note', 'auteur', 'note',
              'note_resume', 'action', 'echeance', 'statut'],
    'contentieux': ['ref_client', 'comp_aux_num', 'responsable',
                    'date_passage', 'commentaire',
                    'provision_risque', 'provision_creances_douteuses'],
}


@st.cache_resource
def get_gspread_client():
    """Se connecte à Google Sheets via le compte de service."""
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(st.secrets["google"]["sheet_id"])


@st.cache_resource
def get_ws(name):
    return get_spreadsheet().worksheet(name)


def _with_retry(fn, *args, **kwargs):
    """Appelle fn avec retry exponentiel sur les erreurs 429 (quota)."""
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            msg = str(e)
            if code == 429 or '429' in msg or 'Quota exceeded' in msg:
                wait = (2 ** attempt) + random.random()
                time.sleep(wait)
                continue
            raise
    raise APIError({"error": {"message": "Quota Google Sheets dépassé après 5 tentatives"}})


@st.cache_resource
def ensure_headers():
    """Écrit les en-têtes dans chaque onglet si absentes. Une seule fois par session."""
    ss = get_spreadsheet()
    existing_sheets = {s.title for s in _with_retry(ss.worksheets)}
    for sheet_name, headers in HEADERS.items():
        if sheet_name not in existing_sheets:
            _with_retry(ss.add_worksheet, title=sheet_name, rows=100, cols=len(headers))
        ws = ss.worksheet(sheet_name)
        first_row = _with_retry(ws.row_values, 1)
        if first_row != headers:
            _with_retry(ws.update, values=[headers], range_name='A1')
    return True


@st.cache_data(ttl=600, show_spinner=False)
def read_sheet(name):
    """Lit un onglet et retourne un DataFrame (cache 10 min pour économiser le quota)."""
    ws = get_ws(name)
    records = _with_retry(ws.get_all_records)
    df = pd.DataFrame(records)
    if df.empty:
        df = pd.DataFrame(columns=HEADERS[name])
    return df


def clear_cache():
    st.cache_data.clear()


def replace_sheet(name, df):
    """Écrase complètement un onglet avec un DataFrame."""
    ws = get_ws(name)
    _with_retry(ws.clear)
    headers = HEADERS[name]
    if df.empty:
        _with_retry(ws.update, values=[headers], range_name='A1')
    else:
        df2 = df.copy()
        for h in headers:
            if h not in df2.columns:
                df2[h] = ''
        df2 = df2[headers].fillna('').astype(str)
        values = [headers] + df2.values.tolist()
        _with_retry(ws.update, values=values, range_name='A1')
    clear_cache()


def append_row(name, row_dict):
    ws = get_ws(name)
    headers = HEADERS[name]
    row = [str(row_dict.get(h, '')) for h in headers]
    _with_retry(ws.append_row, row, value_input_option='USER_ENTERED')
    clear_cache()


def update_cell_by_id(name, row_id, column, new_value):
    """Met à jour une cellule en trouvant la ligne par son id."""
    ws = get_ws(name)
    headers = HEADERS[name]
    col_idx = headers.index(column) + 1
    id_col_idx = headers.index('id') + 1
    cell = ws.find(str(row_id), in_column=id_col_idx)
    if cell:
        ws.update_cell(cell.row, col_idx, new_value)
        clear_cache()


def delete_row_by_id(name, row_id):
    ws = get_ws(name)
    headers = HEADERS[name]
    id_col_idx = headers.index('id') + 1
    cell = ws.find(str(row_id), in_column=id_col_idx)
    if cell:
        ws.delete_rows(cell.row)
        clear_cache()


def next_id(df):
    if df.empty or 'id' not in df.columns:
        return 1
    try:
        return int(pd.to_numeric(df['id'], errors='coerce').max()) + 1
    except (ValueError, TypeError):
        return 1


# ============================================================
# VÉRIFICATION DE LA CONFIG
# ============================================================
def check_config():
    try:
        if "gcp_service_account" not in st.secrets:
            return False, "Secret `gcp_service_account` manquant"
        if "google" not in st.secrets or "sheet_id" not in st.secrets["google"]:
            return False, "Secret `google.sheet_id` manquant"
        ensure_headers()
        return True, "OK"
    except Exception as e:
        return False, f"Erreur connexion Google Sheets : {e}"


# ============================================================
# HELPERS PARSING
# ============================================================
def to_float(val):
    if pd.isna(val) or val is None:
        return 0.0
    try:
        return float(str(val).replace(',', '.').replace(' ', '').replace('\xa0', ''))
    except (ValueError, TypeError):
        return 0.0


def to_str(val):
    if pd.isna(val) or val is None or str(val).lower() == 'nan':
        return ''
    return str(val).strip()


def format_date_fec(d):
    s = to_str(d)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def parse_fec(file_content):
    cols = ['JournalCode', 'JournalLib', 'EcritureNum', 'EcritureDate', 'CompteNum',
            'CompteLib', 'CompAuxNum', 'CompAuxLib', 'PieceRef', 'PieceDate',
            'EcritureLib', 'Debit', 'Credit', 'EcritureLet', 'DateLet', 'ValidDate',
            'Montantdevise', 'Idevise']
    for enc in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(io.BytesIO(file_content), sep='\t', encoding=enc,
                             skiprows=1, header=None, names=cols, dtype=str)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Impossible de décoder le FEC")

    mask = df['CompteNum'].str.startswith('411', na=False)
    clients = df[mask].copy()
    clients['Debit'] = clients['Debit'].apply(to_float)
    clients['Credit'] = clients['Credit'].apply(to_float)
    return clients


def parse_crm(file_content, sheet_name='Liste complète'):
    df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name,
                       header=None, dtype=str)
    headers = df.iloc[1].tolist()
    data = df.iloc[2:].copy()
    data.columns = headers
    data = data[data.iloc[:, 0].astype(str).str.strip() != 'Totaux :']
    data = data[data.iloc[:, 0].notna()]
    return data


def load_creances_enrichies(only_open=True):
    df_c = read_sheet('creances')
    df_m = read_sheet('mapping')
    df_d = read_sheet('dossiers')

    if df_c.empty:
        return pd.DataFrame()

    df_c['debit'] = pd.to_numeric(df_c['debit'], errors='coerce').fillna(0)
    df_c['credit'] = pd.to_numeric(df_c['credit'], errors='coerce').fillna(0)
    df_c['solde'] = df_c['debit'] - df_c['credit']

    if only_open:
        df_c = df_c[(df_c['ecriture_let'].isna()) | (df_c['ecriture_let'] == '')]

        # Auto-rapprochement FIFO par client :
        # Pour chaque client, on prend tous ses encaissements non lettrés (crédits)
        # et on les impute sur ses plus anciennes factures (débits) dans l'ordre.
        # Ainsi, même si l'encaissement n'a pas la même piece_ref que la facture,
        # le rapprochement se fait correctement au niveau du compte client.
        if not df_c.empty:
            kept_rows = []
            # Tri sur date pour FIFO (plus anciennes d'abord)
            df_c['_date_sort'] = pd.to_datetime(df_c['ecriture_date'], errors='coerce')

            for comp_num, grp in df_c.groupby('comp_aux_num', dropna=False):
                debits = grp[grp['debit'] > 0].sort_values('_date_sort').copy()
                credits_sum = grp['credit'].sum()

                # Impute les crédits sur les débits les plus anciens (FIFO)
                remaining = credits_sum
                new_soldes = []
                for _, d in debits.iterrows():
                    montant = d['debit']
                    if remaining >= montant - 0.01:
                        remaining -= montant
                        new_soldes.append(0.0)  # facture totalement soldée
                    elif remaining > 0:
                        new_soldes.append(montant - remaining)
                        remaining = 0
                    else:
                        new_soldes.append(montant)

                debits = debits.assign(solde=new_soldes)
                # On retire les factures soldées
                debits = debits[debits['solde'].abs() > 0.01]
                kept_rows.append(debits)

                # Si après avoir imputé tous les débits il reste du crédit (avoir/trop-percu),
                # on le remonte comme ligne négative (rare)
                if remaining < -0.01:
                    dummy = grp[grp['credit'] > 0].sort_values('_date_sort').head(1).copy()
                    if not dummy.empty:
                        dummy = dummy.assign(solde=-abs(remaining), debit=0, credit=abs(remaining))
                        kept_rows.append(dummy)

            df_c = pd.concat(kept_rows, ignore_index=True) if kept_rows else \
                   df_c.iloc[0:0]
            if '_date_sort' in df_c.columns:
                df_c = df_c.drop(columns=['_date_sort'])

    if not df_m.empty and 'piece_ref' in df_m.columns:
        # Normalise piece_ref des deux côtés (enlève zéros de tête sur chaque segment)
        # pour matcher FEC "22/1" avec PROGEMI "22/0000001"
        def norm_piece(s):
            s = str(s).strip()
            if not s:
                return ''
            parts = s.split('/')
            return '/'.join(p.lstrip('0') or '0' for p in parts)

        df_c['_pk'] = df_c['piece_ref'].apply(norm_piece)
        map_cols = ['piece_ref', 'ref_client']
        if 'date_facture' in df_m.columns:
            map_cols.append('date_facture')
        df_m2 = df_m[map_cols].copy()
        df_m2['_pk'] = df_m2['piece_ref'].apply(norm_piece)
        keep_cols = ['_pk', 'ref_client']
        if 'date_facture' in df_m2.columns:
            keep_cols.append('date_facture')
        df_m2 = df_m2.drop_duplicates('_pk')[keep_cols]
        df_c = df_c.merge(df_m2, on='_pk', how='left').drop(columns=['_pk'])
    else:
        df_c['ref_client'] = ''

    if not df_d.empty:
        dos_cols = ['ref_client', 'client', 'commercial', 'conducteur', 'agence', 'etat',
                    'stade', 'contrat_ttc', 'date_reception']
        df_d_small = df_d[[c for c in dos_cols if c in df_d.columns]]
        df_c = df_c.merge(df_d_small, on='ref_client', how='left')
    else:
        for col in ['client', 'commercial', 'conducteur', 'agence', 'etat', 'stade',
                    'contrat_ttc', 'date_reception']:
            df_c[col] = ''

    # Marque les factures Hors CRM pour les distinguer des non-rattachées
    mask_hors = df_c['ref_client'] == '__HORS_CRM__'
    df_c.loc[mask_hors, 'ref_client'] = 'Hors CRM'
    df_c.loc[mask_hors, 'client'] = df_c.loc[mask_hors, 'comp_aux_lib']

    # Jours de retard : priorité à la date de facture du PROGEMI (date_facture),
    # puis piece_date du FEC, puis fallback ecriture_date
    today = pd.Timestamp(datetime.now().date())
    if 'date_facture' in df_c.columns:
        df_c['_dt_progemi'] = pd.to_datetime(df_c['date_facture'], errors='coerce')
    else:
        df_c['_dt_progemi'] = pd.NaT
    if 'piece_date' in df_c.columns:
        df_c['_dt_piece'] = pd.to_datetime(df_c['piece_date'], errors='coerce')
    else:
        df_c['_dt_piece'] = pd.NaT
    df_c['_dt_ecr'] = pd.to_datetime(df_c['ecriture_date'], errors='coerce')
    df_c['_dt'] = df_c['_dt_progemi'].fillna(df_c['_dt_piece']).fillna(df_c['_dt_ecr'])
    df_c['jours_retard'] = (today - df_c['_dt']).dt.days
    df_c['jours_retard'] = df_c['jours_retard'].fillna(0).astype(int).clip(lower=0)
    df_c = df_c.drop(columns=['_dt', '_dt_progemi', '_dt_piece', '_dt_ecr'])

    # Flag contentieux + responsable + provisions
    df_ct = read_sheet('contentieux')
    if not df_ct.empty and 'ref_client' in df_ct.columns:
        ct_cols = ['ref_client', 'responsable']
        for col in ['provision_risque', 'provision_creances_douteuses']:
            if col in df_ct.columns:
                ct_cols.append(col)
        df_ct_small = df_ct[ct_cols].drop_duplicates('ref_client')
        # Cast provisions en numérique
        for col in ['provision_risque', 'provision_creances_douteuses']:
            if col in df_ct_small.columns:
                df_ct_small[col] = pd.to_numeric(df_ct_small[col], errors='coerce').fillna(0)
        df_c = df_c.merge(df_ct_small, on='ref_client', how='left')
        df_c['contentieux'] = df_c['responsable'].notna() & (df_c['responsable'] != '')
        df_c['responsable'] = df_c['responsable'].fillna('')
        for col in ['provision_risque', 'provision_creances_douteuses']:
            if col in df_c.columns:
                df_c[col] = pd.to_numeric(df_c[col], errors='coerce').fillna(0)
            else:
                df_c[col] = 0
    else:
        df_c['contentieux'] = False
        df_c['responsable'] = ''
        df_c['provision_risque'] = 0
        df_c['provision_creances_douteuses'] = 0

    return df_c.sort_values('solde', ascending=False)


# ============================================================
# PAGES
# ============================================================
def page_import():
    st.header("📥 Import des données")

    tab1, tab2, tab3, tab4 = st.tabs(["FEC", "CRM (Chantiers)",
                                       "Mapping factures", "Contentieux"])

    with tab1:
        st.markdown("**Import du Fichier d'Écritures Comptables**")
        st.caption("Extrait les comptes 411xxx. Les écritures lettrées sont marquées comme soldées.")
        fec_file = st.file_uploader("Fichier FEC (.txt)", type=['txt'], key='fec')
        if fec_file and st.button("Importer le FEC", type="primary"):
            try:
                with st.spinner("Analyse du FEC et écriture dans Google Sheets..."):
                    clients = parse_fec(fec_file.read())
                    rows = []
                    for i, (_, r) in enumerate(clients.iterrows(), 1):
                        rows.append({
                            'id': i,
                            'comp_aux_num': r['CompAuxNum'],
                            'comp_aux_lib': r['CompAuxLib'],
                            'piece_ref': r['PieceRef'],
                            'piece_date': format_date_fec(r['PieceDate']),
                            'ecriture_date': format_date_fec(r['EcritureDate']),
                            'journal_code': r['JournalCode'],
                            'ecriture_lib': r['EcritureLib'],
                            'debit': r['Debit'],
                            'credit': r['Credit'],
                            'ecriture_let': to_str(r['EcritureLet']),
                            'import_date': datetime.now().isoformat(),
                        })
                    df_new = pd.DataFrame(rows)
                    replace_sheet('creances', df_new)
                st.success(f"✅ {len(clients)} écritures clients importées")
            except Exception as e:
                st.error(f"Erreur : {e}")

    with tab2:
        st.markdown("**Import de l'export CRM (Chantiers)**")
        crm_file = st.file_uploader("Export CRM (.xlsx)", type=['xlsx'], key='crm')
        if crm_file:
            try:
                xl = pd.ExcelFile(io.BytesIO(crm_file.getvalue()))
                default_idx = xl.sheet_names.index('Liste complète') \
                    if 'Liste complète' in xl.sheet_names else 0
                sheet = st.selectbox("Feuille à importer", xl.sheet_names, index=default_idx)
                if st.button("Importer le CRM", type="primary"):
                    with st.spinner("Analyse du CRM et écriture dans Google Sheets..."):
                        data = parse_crm(crm_file.getvalue(), sheet_name=sheet)

                        # Lookup tolérant aux accents/casse/espaces
                        import unicodedata as _ud, re as _re3
                        def _norm_col(s):
                            s = str(s).strip().lower()
                            s = _ud.normalize('NFKD', s).encode('ascii', 'ignore').decode()
                            return _re3.sub(r'[^a-z0-9]+', '', s)
                        _col_lookup = {_norm_col(c): c for c in data.columns}

                        def g(row, col):
                            key = _norm_col(col)
                            actual = _col_lookup.get(key)
                            if actual is None:
                                return ''
                            return to_str(row.get(actual, ''))

                        def gf(row, col):
                            key = _norm_col(col)
                            actual = _col_lookup.get(key)
                            if actual is None:
                                return ''
                            v = row.get(actual, '')
                            if pd.isna(v) or str(v).lower() == 'nan' or str(v).strip() == '':
                                return ''
                            return to_float(v)

                        # Debug : affiche les colonnes détectées
                        with st.expander("🔍 Colonnes détectées dans le fichier CRM"):
                            st.write(list(data.columns))

                        rows = []
                        for _, r in data.iterrows():
                            ref = g(r, 'Ref client')
                            if not ref:
                                continue
                            rows.append({
                                'ref_client': ref,
                                'code_affaire': g(r, 'N°Compta/Code Affaire'),
                                'client': g(r, 'Client(s)'),
                                'email1': g(r, 'Client Email 1'),
                                'email2': g(r, 'Client Email 2'),
                                'type_projet': g(r, 'Type de projet'),
                                'adresse': g(r, 'Adresse du projet'),
                                'cp': g(r, 'CP'),
                                'ville': g(r, 'Ville'),
                                'constructeur': g(r, 'Constructeur'),
                                'agence': g(r, 'Agence'),
                                'commercial': g(r, 'Commercial'),
                                'conducteur': g(r, 'Conducteur de travaux'),
                                'etat': g(r, 'Etat'),
                                'stade': g(r, "Stade d'avancement"),
                                'type_contrat': g(r, 'Type de contrat'),
                                'contrat_ht': gf(r, 'Contrat HT'),
                                'contrat_ttc': gf(r, 'Contrat TTC'),
                                'contrat_rev_ht': gf(r, 'Contrat révisé HT'),
                                'contrat_rev_ttc': gf(r, 'Contrat révisé TTC'),
                                'avenants_ht': gf(r, 'Avenants HT'),
                                'avenants_ttc': gf(r, 'Avenants TTC'),
                                'date_signature': g(r, 'Date de signature du contrat'),
                                'date_reception': g(r, 'Date de réception'),
                            })
                        replace_sheet('dossiers', pd.DataFrame(rows))
                    st.success(f"✅ {len(rows)} dossiers importés")
            except Exception as e:
                st.error(f"Erreur : {e}")

    with tab3:
        st.markdown("**Import du fichier de facturation** (lien facture ↔ dossier CRM)")
        st.info(
            "Fichier CSV/Excel contenant les correspondances entre numéros de facture "
            "(présents dans le FEC) et références dossier (présentes dans le CRM).\n\n"
            "**Colonnes attendues** :\n"
            "- `piece_ref` — numéro de facture (ex: `26/0000002`, `21/52`)\n"
            "- `ref_client` — référence dossier CRM (ex: `00655`, `976`)\n"
            "- `comp_aux_num` — *optionnel*, code comptable client (ex: `CROBERT`)\n\n"
            "💡 Les noms de colonnes peuvent varier : `numero_facture`, `num_facture`, "
            "`facture`, `ref_dossier`, `dossier` sont aussi reconnus."
        )

        map_file = st.file_uploader("Fichier de facturation (.csv ou .xlsx)",
                                    type=['csv', 'xlsx'], key='map')

        # Option pour cumuler (ajouter) ou remplacer
        mode = st.radio("Mode d'import",
                        ["Remplacer tout le mapping existant",
                         "Ajouter / mettre à jour (cumul multi-années)"],
                        horizontal=True, key='map_mode')

        if map_file and st.button("Importer le fichier de facturation", type="primary"):
            try:
                # Aliases (normalisés sans accent ni espace)
                aliases_piece = ['piece_ref', 'numero_facture', 'num_facture',
                                 'facture', 'n_facture', 'no_facture', 'piece',
                                 'nfacture', 'numfacture']
                aliases_ref = ['ref_client', 'ref_dossier', 'dossier', 'ref',
                               'code_affaire', 'n_compta', 'code_dossier',
                               'codedossier', 'refclient', 'refdossier']
                aliases_comp = ['comp_aux_num', 'code_client', 'code_compta',
                                'code_comptable']
                aliases_date = ['date', 'date_facture', 'datefacture',
                                'date_fact', 'date_piece', 'date_emission']

                def normalize(s):
                    import unicodedata, re
                    s = str(s).strip().lower()
                    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
                    s = re.sub(r'[^a-z0-9]+', '_', s).strip('_')
                    return s

                # Détection auto de la ligne d'en-tête (cherche "n° facture" ou similaire)
                df_map = None
                detected_header = None
                for header_row in range(0, 5):
                    try:
                        if map_file.name.endswith('.csv'):
                            tmp = pd.read_csv(map_file, dtype=str, header=header_row)
                        else:
                            map_file.seek(0)
                            tmp = pd.read_excel(map_file, dtype=str, header=header_row)
                        norm_cols = [normalize(c) for c in tmp.columns]
                        if any(c in aliases_piece for c in norm_cols) and \
                           any(c in aliases_ref for c in norm_cols):
                            df_map = tmp
                            df_map.columns = norm_cols
                            detected_header = header_row
                            break
                    except Exception:
                        continue

                if df_map is None:
                    st.error("Impossible de détecter les colonnes `facture` et `dossier` "
                             "dans le fichier. Vérifiez les en-têtes.")
                else:
                    col_piece = next(c for c in aliases_piece if c in df_map.columns)
                    col_ref = next(c for c in aliases_ref if c in df_map.columns)
                    col_comp = next((c for c in aliases_comp if c in df_map.columns), None)
                    col_date = next((c for c in aliases_date if c in df_map.columns), None)

                    st.info(f"✅ En-tête détecté en ligne {detected_header + 1}. "
                            f"Colonnes utilisées : facture=`{col_piece}`, "
                            f"dossier=`{col_ref}`"
                            + (f", client=`{col_comp}`" if col_comp else "")
                            + (f", date=`{col_date}`" if col_date else ""))

                    def _fmt_date(v):
                        """Parse une date (str ou datetime) vers YYYY-MM-DD."""
                        if v is None or str(v).strip() == '' or str(v).lower() == 'nan':
                            return ''
                        try:
                            d = pd.to_datetime(v, errors='coerce', dayfirst=True)
                            if pd.isna(d):
                                return ''
                            return d.date().isoformat()
                        except Exception:
                            return ''

                    # Construit un index des refs CRM : gère les dossiers regroupés (ex CRM "830/831")
                    # et normalise les zéros de tête (ex PROGEMI "830" <-> CRM "00830").
                    # Chaque sous-ref d'un dossier CRM groupé pointe vers la ref CRM complète.
                    df_d_current = read_sheet('dossiers')
                    crm_refs = df_d_current['ref_client'].astype(str).tolist() \
                        if not df_d_current.empty and 'ref_client' in df_d_current.columns else []
                    import re as _re
                    crm_index = {}  # clé = sous-ref normalisée sans zéros, valeur = ref CRM d'origine
                    for cr in crm_refs:
                        cr_clean = cr.strip()
                        if not cr_clean:
                            continue
                        # Éclate les dossiers groupés côté CRM et indexe chaque sous-ref
                        for sub in _re.split(r'[/,;]+', cr_clean):
                            sub = sub.strip()
                            if sub:
                                crm_index[sub.lstrip('0') or '0'] = cr_clean
                        # Indexe aussi la ref complète telle quelle (au cas où PROGEMI contient "830/831")
                        crm_index[cr_clean.lstrip('0') or '0'] = cr_clean

                    def resolve_ref(raw):
                        """Renvoie la ref CRM (éventuellement groupée) si trouvée, sinon la ref brute."""
                        raw = str(raw).strip()
                        if not raw:
                            return ''
                        key = raw.lstrip('0') or '0'
                        return crm_index.get(key, raw)

                    rows = []
                    nb_groupes_crm = sum(1 for cr in crm_refs if _re.search(r'[/,;]', cr or ''))
                    nb_non_resolus = 0
                    crm_values = set(crm_index.values())
                    for _, r in df_map.iterrows():
                        pr = to_str(r.get(col_piece, ''))
                        rc_raw = to_str(r.get(col_ref, ''))
                        if not pr or not rc_raw or pr.lower() == 'nan':
                            continue
                        resolved = resolve_ref(rc_raw)
                        if resolved not in crm_values:
                            nb_non_resolus += 1
                        rows.append({
                            'piece_ref': pr,
                            'ref_client': resolved,
                            'comp_aux_num': to_str(r.get(col_comp, '')) if col_comp else '',
                            'date_facture': _fmt_date(r.get(col_date, '')) if col_date else '',
                        })

                    new_df = pd.DataFrame(rows)
                    if nb_groupes_crm:
                        st.info(f"🔀 {nb_groupes_crm} dossiers CRM regroupés détectés "
                                f"(ex: `830/831`) — les factures PROGEMI `830` et `831` "
                                f"pointeront toutes vers le dossier groupé")
                    if nb_non_resolus and crm_values:
                        st.warning(f"⚠️ {nb_non_resolus} refs PROGEMI non trouvées dans le CRM "
                                   f"(importez d'abord le CRM ou vérifiez les codes)")

                    if mode.startswith("Ajouter"):
                        existing = read_sheet('mapping')
                        if not existing.empty:
                            # Upsert : on retire les piece_ref déjà présentes, on ajoute les nouvelles
                            existing = existing[~existing['piece_ref'].isin(new_df['piece_ref'])]
                            merged = pd.concat([existing, new_df], ignore_index=True)
                        else:
                            merged = new_df
                        replace_sheet('mapping', merged)
                        st.success(f"✅ {len(rows)} correspondances importées "
                                   f"(total : {len(merged)})")
                    else:
                        replace_sheet('mapping', new_df)
                        st.success(f"✅ {len(rows)} correspondances facture → dossier importées")
            except Exception as e:
                st.error(f"Erreur : {e}")

        st.divider()
        st.markdown("**Affectation manuelle** — factures non rattachées à un dossier")
        st.caption("Les choix sont sauvegardés dans Google Sheets et persistent à chaque import. "
                   "Marquez une facture « Hors CRM » pour qu'elle ne réapparaisse plus.")

        df_c = read_sheet('creances')
        df_m = read_sheet('mapping')
        df_d = read_sheet('dossiers')

        # Même fonction de normalisation que dans load_creances_enrichies
        def _norm_piece(s):
            s = str(s).strip()
            if not s:
                return ''
            return '/'.join(p.lstrip('0') or '0' for p in s.split('/'))

        if not df_c.empty and not df_d.empty:
            df_c['debit'] = pd.to_numeric(df_c['debit'], errors='coerce').fillna(0)
            df_c['credit'] = pd.to_numeric(df_c['credit'], errors='coerce').fillna(0)
            df_c['solde'] = df_c['debit'] - df_c['credit']
            df_c = df_c[(df_c['ecriture_let'].isna()) | (df_c['ecriture_let'] == '')]
            df_c = df_c[df_c['solde'] > 0]

            # Compare sur clé normalisée pour gérer 22/1 vs 22/0000001
            mapped_keys = set(df_m['piece_ref'].apply(_norm_piece).tolist()) \
                if not df_m.empty else set()
            df_c['_pk'] = df_c['piece_ref'].apply(_norm_piece)
            non_map = df_c[~df_c['_pk'].isin(mapped_keys)]
            non_map = non_map.groupby(['piece_ref', 'comp_aux_num', 'comp_aux_lib']).agg(
                solde=('solde', 'sum'),
                date=('ecriture_date', 'first')
            ).reset_index().sort_values('solde', ascending=False)

            if non_map.empty:
                st.success("✅ Toutes les factures ouvertes sont rattachées (ou marquées Hors CRM).")
            else:
                total_non_map = non_map['solde'].sum()
                c_a, c_b = st.columns(2)
                c_a.metric("Factures à traiter", len(non_map))
                c_b.metric("Montant concerné", f"{total_non_map:,.0f} €".replace(",", " "))

                # Auto-classification Hors CRM par motif (préfixe de piece_ref)
                with st.expander("⚡ Auto-classer des factures comme Hors CRM (par motif)"):
                    import re as _re2
                    pattern = st.text_input(
                        "Motif regex sur le n° de facture",
                        value=r"^FC",
                        help="Exemples : `^FC` pour toutes les factures commençant par FC, "
                             "`^(FC|AV)` pour FC ou AV, `^FC\\d+$` pour FC suivi de chiffres uniquement"
                    )
                    try:
                        rx = _re2.compile(pattern, _re2.IGNORECASE)
                        preview = non_map[non_map['piece_ref'].apply(
                            lambda x: bool(rx.search(str(x))))]
                    except _re2.error as e:
                        st.error(f"Motif invalide : {e}")
                        preview = pd.DataFrame()

                    if not preview.empty:
                        st.write(f"**{len(preview)} factures** correspondraient au motif "
                                 f"(total : {preview['solde'].sum():,.0f} €)".replace(",", " "))
                        st.dataframe(
                            preview[['piece_ref', 'comp_aux_lib', 'solde']].head(10),
                            use_container_width=True, hide_index=True
                        )
                        if st.button(f"⊘ Marquer ces {len(preview)} factures Hors CRM",
                                     type="primary", key="btn_auto_hors"):
                            new_rows = pd.DataFrame([{
                                'piece_ref': r['piece_ref'],
                                'ref_client': '__HORS_CRM__',
                                'comp_aux_num': r['comp_aux_num'],
                            } for _, r in preview.iterrows()])
                            # retire d'abord les éventuels mappings existants sur ces pieces
                            keys_to_remove = set(preview['piece_ref'].apply(_norm_piece))
                            existing = df_m[~df_m['piece_ref'].apply(_norm_piece)
                                             .isin(keys_to_remove)] \
                                if not df_m.empty else pd.DataFrame(columns=HEADERS['mapping'])
                            merged = pd.concat([existing, new_rows], ignore_index=True)
                            replace_sheet('mapping', merged)
                            st.success(f"✅ {len(preview)} factures marquées Hors CRM")
                            st.rerun()
                    elif pattern:
                        st.info("Aucune facture ne correspond à ce motif.")

                # Recherche + pagination
                q = st.text_input("🔎 Rechercher (n° facture, client, code compta)",
                                  key="search_nonmap").strip().lower()
                if q:
                    mask = (non_map['piece_ref'].str.lower().str.contains(q, na=False)
                            | non_map['comp_aux_lib'].str.lower().str.contains(q, na=False)
                            | non_map['comp_aux_num'].str.lower().str.contains(q, na=False))
                    non_map = non_map[mask]

                per_page = 25
                nb_pages = max(1, (len(non_map) + per_page - 1) // per_page)
                page = st.number_input(f"Page (1 à {nb_pages})", min_value=1,
                                       max_value=nb_pages, value=1, step=1,
                                       key="page_nonmap")
                start = (page - 1) * per_page
                page_df = non_map.iloc[start:start + per_page]

                # Options dossiers CRM + option spéciale Hors CRM
                HORS_CRM = "__HORS_CRM__"
                options_labels = ["— Choisir un dossier —",
                                  "⊘ Hors CRM (facture sans dossier)"]
                options_vals = ["", HORS_CRM]
                for _, r in df_d.iterrows():
                    if r['ref_client']:
                        options_labels.append(f"{r['ref_client']} — {r['client']}")
                        options_vals.append(r['ref_client'])

                st.divider()
                for _, row in page_df.iterrows():
                    c1, c2, c3 = st.columns([2, 3, 1])
                    c1.write(f"**{row['piece_ref']}**")
                    c1.caption(f"{row['comp_aux_num']} — {row['comp_aux_lib']}")
                    idx = c2.selectbox(
                        "Dossier", range(len(options_labels)),
                        format_func=lambda i: options_labels[i],
                        key=f"map_{row['piece_ref']}",
                        label_visibility="collapsed")
                    c3.write(f"{row['solde']:,.0f} €".replace(",", " "))
                    if idx > 0:  # 0 = placeholder "— Choisir —"
                        ref_val = options_vals[idx]
                        new_row = pd.DataFrame([{
                            'piece_ref': row['piece_ref'],
                            'ref_client': ref_val,
                            'comp_aux_num': row['comp_aux_num'],
                        }])
                        existing = df_m[df_m['piece_ref'].apply(_norm_piece)
                                        != _norm_piece(row['piece_ref'])] \
                            if not df_m.empty else pd.DataFrame(columns=HEADERS['mapping'])
                        df_m_updated = pd.concat([existing, new_row], ignore_index=True)
                        replace_sheet('mapping', df_m_updated)
                        st.rerun()

            # --- Section Hors CRM : permet de revenir en arrière ---
            if not df_m.empty and (df_m['ref_client'] == '__HORS_CRM__').any():
                with st.expander("⊘ Factures marquées Hors CRM (cliquer pour réaffecter)"):
                    hors = df_m[df_m['ref_client'] == '__HORS_CRM__']
                    st.write(f"{len(hors)} facture(s) marquée(s) Hors CRM")
                    for _, hr in hors.iterrows():
                        cc1, cc2 = st.columns([3, 1])
                        cc1.write(f"**{hr['piece_ref']}** ({hr['comp_aux_num']})")
                        if cc2.button("↶ Annuler", key=f"unhors_{hr['piece_ref']}"):
                            df_m_cleaned = df_m[df_m['piece_ref'] != hr['piece_ref']]
                            replace_sheet('mapping', df_m_cleaned)
                            st.rerun()

    with tab4:
        st.markdown("**Gestion des dossiers en contentieux**")
        st.caption("Les dossiers listés ici sont exclus de l'export commerciaux "
                   "et apparaissent dans un export dédié.")

        df_ct = read_sheet('contentieux')
        df_d_ct = read_sheet('dossiers')
        df_c_ct = read_sheet('creances')

        # --- Formulaire d'ajout ---
        st.markdown("### Ajouter un dossier au contentieux")

        # Construit la liste des dossiers disponibles (CRM + clients FEC sans dossier)
        already = set(df_ct['ref_client'].tolist()) if not df_ct.empty else set()

        options_add_labels = ["— Choisir un dossier —"]
        options_add_vals = [None]

        if not df_d_ct.empty:
            for _, r in df_d_ct.iterrows():
                if r['ref_client'] and r['ref_client'] not in already:
                    options_add_labels.append(
                        f"CRM — {r['ref_client']} — {r['client']}")
                    options_add_vals.append({
                        'ref_client': r['ref_client'],
                        'comp_aux_num': '',
                    })

        # Permet aussi d'ajouter un client FEC sans dossier CRM (via comp_aux_num)
        if not df_c_ct.empty:
            clients_fec = df_c_ct[['comp_aux_num', 'comp_aux_lib']] \
                .drop_duplicates('comp_aux_num')
            for _, r in clients_fec.iterrows():
                label = f"FEC — {r['comp_aux_num']} — {r['comp_aux_lib']}"
                key_val = f"FEC:{r['comp_aux_num']}"
                if key_val not in already:
                    options_add_labels.append(label)
                    options_add_vals.append({
                        'ref_client': key_val,
                        'comp_aux_num': r['comp_aux_num'],
                    })

        c_a, c_b, c_c = st.columns([3, 2, 3])
        idx_sel = c_a.selectbox("Dossier", range(len(options_add_labels)),
                                format_func=lambda i: options_add_labels[i],
                                key="ct_add_dossier")
        resp = c_b.text_input("Responsable", key="ct_add_resp",
                              placeholder="Nom du gestionnaire")
        comm = c_c.text_input("Commentaire", key="ct_add_comm",
                              placeholder="Facultatif")

        c_d, c_e = st.columns(2)
        prov_r = c_d.number_input("Provision pour risque (€)",
                                  min_value=0.0, step=100.0, value=0.0,
                                  key="ct_add_prov_r")
        prov_cd = c_e.number_input("Provision créances douteuses (€)",
                                   min_value=0.0, step=100.0, value=0.0,
                                   key="ct_add_prov_cd")

        if st.button("➕ Ajouter au contentieux", type="primary"):
            if idx_sel == 0:
                st.warning("Sélectionnez un dossier.")
            elif not resp.strip():
                st.warning("Le responsable est obligatoire.")
            else:
                payload = options_add_vals[idx_sel]
                new_row = pd.DataFrame([{
                    'ref_client': payload['ref_client'],
                    'comp_aux_num': payload['comp_aux_num'],
                    'responsable': resp.strip(),
                    'date_passage': datetime.now().date().isoformat(),
                    'commentaire': comm.strip(),
                    'provision_risque': prov_r,
                    'provision_creances_douteuses': prov_cd,
                }])
                merged = pd.concat([df_ct, new_row], ignore_index=True) \
                    if not df_ct.empty else new_row
                replace_sheet('contentieux', merged)
                st.success("✅ Ajouté au contentieux.")
                st.rerun()

        # --- Liste des dossiers en contentieux (édition provisions) ---
        st.markdown("### Dossiers actuellement en contentieux")
        if df_ct.empty:
            st.info("Aucun dossier en contentieux.")
        else:
            st.caption(f"{len(df_ct)} dossier(s) — éditez les provisions ci-dessous puis "
                       "cliquez **Enregistrer**.")

            # Forme un dataframe éditable
            df_ct_edit = df_ct.copy()
            for col in ['provision_risque', 'provision_creances_douteuses']:
                if col not in df_ct_edit.columns:
                    df_ct_edit[col] = 0
                df_ct_edit[col] = pd.to_numeric(df_ct_edit[col], errors='coerce').fillna(0)

            edited = st.data_editor(
                df_ct_edit[['ref_client', 'responsable', 'date_passage',
                            'commentaire', 'provision_risque',
                            'provision_creances_douteuses']],
                column_config={
                    'ref_client': st.column_config.TextColumn('Dossier', disabled=True),
                    'responsable': st.column_config.TextColumn('Responsable'),
                    'date_passage': st.column_config.TextColumn('Date passage', disabled=True),
                    'commentaire': st.column_config.TextColumn('Commentaire'),
                    'provision_risque': st.column_config.NumberColumn(
                        'Prov. risque (€)', min_value=0, step=100, format="%.2f"),
                    'provision_creances_douteuses': st.column_config.NumberColumn(
                        'Prov. créances douteuses (€)',
                        min_value=0, step=100, format="%.2f"),
                },
                use_container_width=True, hide_index=True, key="ct_editor",
                num_rows="fixed",
            )

            colb1, colb2 = st.columns([1, 5])
            if colb1.button("💾 Enregistrer", type="primary"):
                # Réinjecte les valeurs éditées dans df_ct (en gardant comp_aux_num)
                df_ct_new = df_ct.copy()
                for c in ['responsable', 'commentaire', 'provision_risque',
                          'provision_creances_douteuses']:
                    df_ct_new[c] = edited[c].values
                replace_sheet('contentieux', df_ct_new)
                st.success("✅ Modifications enregistrées.")
                st.rerun()

            st.markdown("**Retirer un dossier du contentieux :**")
            for i, r in df_ct.iterrows():
                cc1, cc2 = st.columns([5, 1])
                cc1.write(f"{r['ref_client']} — 👤 {r['responsable']}")
                if cc2.button("🗑️", key=f"del_ct_{i}", help="Retirer du contentieux"):
                    df_ct_cleaned = df_ct.drop(index=i).reset_index(drop=True)
                    replace_sheet('contentieux', df_ct_cleaned)
                    st.rerun()

    st.divider()
    df_c = read_sheet('creances')
    df_d = read_sheet('dossiers')
    df_m = read_sheet('mapping')
    n_cr = len(df_c)
    n_cr_ouv = 0
    if not df_c.empty and 'ecriture_let' in df_c.columns:
        n_cr_ouv = len(df_c[(df_c['ecriture_let'].isna()) | (df_c['ecriture_let'] == '')])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Écritures FEC", n_cr)
    c2.metric("Non lettrées", n_cr_ouv)
    c3.metric("Dossiers CRM", len(df_d))
    c4.metric("Mappings", len(df_m))


def page_creances():
    st.header("📊 Créances ouvertes")

    df = load_creances_enrichies(only_open=True)
    if df.empty:
        st.warning("Aucune créance. Importez le FEC dans l'onglet Import.")
        return

    df = df[df['solde'].abs() > 0.01]

    if 'conducteur' not in df.columns:
        df['conducteur'] = ''

    c1, c2, c3, c4, c5 = st.columns(5)
    coms = ['(Tous)'] + sorted([c for c in df['commercial'].dropna().unique() if c])
    filt_com = c1.selectbox("Commercial", coms)
    conds = ['(Tous)'] + sorted([c for c in df['conducteur'].dropna().unique() if c])
    filt_cond = c2.selectbox("Conducteur", conds)
    ags = ['(Toutes)'] + sorted([a for a in df['agence'].dropna().unique() if a])
    filt_ag = c3.selectbox("Agence", ags)
    etats = ['(Tous)'] + sorted([e for e in df['etat'].dropna().unique() if e])
    filt_et = c4.selectbox("État dossier", etats)
    seuil = c5.number_input("Solde mini (€)", value=0, step=500)

    f = df.copy()
    if filt_com != '(Tous)':
        f = f[f['commercial'] == filt_com]
    if filt_cond != '(Tous)':
        f = f[f['conducteur'] == filt_cond]
    if filt_ag != '(Toutes)':
        f = f[f['agence'] == filt_ag]
    if filt_et != '(Tous)':
        f = f[f['etat'] == filt_et]
    f = f[f['solde'] >= seuil]

    # Sépare les dossiers en contentieux du reste
    f_contentieux = f[f['contentieux']].copy() if 'contentieux' in f.columns else pd.DataFrame()
    f = f[~f['contentieux']] if 'contentieux' in f.columns else f

    total = f['solde'].sum()
    nb_cli = f['comp_aux_num'].nunique()
    nb_mappes = f['ref_client'].fillna('').astype(bool).sum()
    non_mappe = f[f['ref_client'].fillna('') == '']['solde'].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Solde total dû", f"{total:,.0f} €".replace(",", " "))
    k2.metric("Clients concernés", nb_cli)
    k3.metric("Lignes rattachées", f"{nb_mappes} / {len(f)}")
    k4.metric("Non rattaché (€)", f"{non_mappe:,.0f}".replace(",", " "))

    st.subheader("Synthèse par client")
    synth = f.groupby(['comp_aux_num', 'comp_aux_lib', 'ref_client', 'client',
                       'commercial', 'conducteur', 'agence', 'etat'], dropna=False).agg(
        solde=('solde', 'sum'),
        nb=('piece_ref', 'count'),
        derniere_ecriture=('ecriture_date', 'max'),
        jours_retard=('jours_retard', 'max')
    ).reset_index().sort_values('solde', ascending=False)

    # Coloration conditionnelle jours de retard : vert <7, orange 7-29, rouge >=30
    def _color_retard(v):
        try:
            v = int(v)
        except Exception:
            return ''
        if v <= 0:
            return ''
        if v < 7:
            return 'background-color: #C8E6C9; color: #1B5E20;'  # vert
        if v < 30:
            return 'background-color: #FFE0B2; color: #E65100;'  # orange
        return 'background-color: #FFCDD2; color: #B71C1C;'  # rouge

    synth_display = synth.rename(columns={
        'comp_aux_num': 'Code compta', 'comp_aux_lib': 'Client FEC',
        'ref_client': 'Ref dossier', 'client': 'Client CRM',
        'commercial': 'Commercial', 'conducteur': 'Conducteur',
        'agence': 'Agence',
        'etat': 'État', 'solde': 'Solde (€)',
        'nb': 'Nb lignes', 'derniere_ecriture': 'Dernière écriture',
        'jours_retard': 'Jours retard'
    })
    styled = synth_display.style.map(_color_retard, subset=['Jours retard']) \
        .format({'Solde (€)': '{:.2f}'})
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Sous-tableau Contentieux
    if not f_contentieux.empty:
        st.subheader("⚖️ Dossiers en contentieux")
        synth_ct = f_contentieux.groupby(
            ['comp_aux_num', 'comp_aux_lib', 'ref_client', 'client',
             'responsable', 'commercial', 'agence'], dropna=False).agg(
            solde=('solde', 'sum'),
            nb=('piece_ref', 'count'),
            derniere_ecriture=('ecriture_date', 'max'),
            jours_retard=('jours_retard', 'max'),
            provision_risque=('provision_risque', 'first'),
            provision_creances_douteuses=('provision_creances_douteuses', 'first'),
        ).reset_index().sort_values('solde', ascending=False)

        total_ct = synth_ct['solde'].sum()
        total_pr = synth_ct['provision_risque'].sum()
        total_pcd = synth_ct['provision_creances_douteuses'].sum()
        ka, kb, kc, kd = st.columns(4)
        ka.metric("Total contentieux", f"{total_ct:,.0f} €".replace(",", " "))
        kb.metric("Dossiers", len(synth_ct))
        kc.metric("Prov. risque", f"{total_pr:,.0f} €".replace(",", " "))
        kd.metric("Prov. créances douteuses", f"{total_pcd:,.0f} €".replace(",", " "))

        synth_ct_display = synth_ct.rename(columns={
            'comp_aux_num': 'Code compta', 'comp_aux_lib': 'Client FEC',
            'ref_client': 'Ref dossier', 'client': 'Client CRM',
            'responsable': 'Responsable',
            'commercial': 'Commercial', 'agence': 'Agence',
            'solde': 'Solde (€)', 'nb': 'Nb lignes',
            'derniere_ecriture': 'Dernière écriture',
            'jours_retard': 'Jours retard',
            'provision_risque': 'Prov. risque (€)',
            'provision_creances_douteuses': 'Prov. créances douteuses (€)',
        })
        styled_ct = synth_ct_display.style.map(_color_retard, subset=['Jours retard']) \
            .format({'Solde (€)': '{:.2f}',
                     'Prov. risque (€)': '{:.2f}',
                     'Prov. créances douteuses (€)': '{:.2f}'})
        st.dataframe(styled_ct, use_container_width=True, hide_index=True)

    with st.expander("Détail ligne par ligne"):
        st.dataframe(
            f[['comp_aux_lib', 'piece_ref', 'ecriture_date', 'journal_code',
               'ecriture_lib', 'debit', 'credit', 'solde', 'commercial', 'agence', 'etat']]
            .rename(columns={
                'comp_aux_lib': 'Client', 'piece_ref': 'Réf. pièce',
                'ecriture_date': 'Date', 'journal_code': 'Journal',
                'ecriture_lib': 'Libellé', 'debit': 'Débit',
                'credit': 'Crédit', 'solde': 'Solde',
                'commercial': 'Commercial', 'agence': 'Agence', 'etat': 'État'
            }),
            use_container_width=True, hide_index=True
        )


def page_notes():
    st.header("📝 Notes & Relances")

    df_full = load_creances_enrichies(only_open=True)
    if df_full.empty:
        st.warning("Aucune créance ouverte. Importez le FEC.")
        return

    df_full = df_full[df_full['solde'] > 0.01]
    clients = df_full.groupby(['comp_aux_num', 'comp_aux_lib']).agg(
        solde=('solde', 'sum'),
        client=('client', 'first'),
        commercial=('commercial', 'first'),
        ref_client=('ref_client', 'first'),
    ).reset_index().sort_values('solde', ascending=False)

    notes_df = read_sheet('notes')
    if not notes_df.empty:
        notes_df['id'] = pd.to_numeric(notes_df['id'], errors='coerce')

    labels = {f"{r['comp_aux_lib']} — {r['solde']:,.0f} €".replace(",", " "): r['comp_aux_num']
              for _, r in clients.iterrows()}
    sel = st.selectbox("Client", ['— Vue globale —'] + list(labels.keys()))

    if sel == '— Vue globale —':
        st.subheader(f"Toutes les relances ({len(notes_df)})")
        if notes_df.empty:
            st.info("Aucune note.")
        else:
            enriched = notes_df.merge(
                clients[['comp_aux_num', 'client', 'commercial']],
                on='comp_aux_num', how='left')
            st.dataframe(
                enriched[['date_note', 'comp_aux_num', 'client', 'commercial',
                          'auteur', 'action', 'note', 'echeance', 'statut']],
                use_container_width=True, hide_index=True)
        return

    comp_aux_num = labels[sel]
    info = clients[clients['comp_aux_num'] == comp_aux_num].iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Solde dû", f"{info['solde']:,.2f} €".replace(",", " "))
    c2.write(f"**Client CRM :** {info['client'] or '(non rattaché)'}")
    c3.write(f"**Commercial :** {info['commercial'] or '—'}")

    fac = df_full[df_full['comp_aux_num'] == comp_aux_num][
        ['piece_ref', 'ecriture_date', 'ecriture_lib', 'debit', 'credit', 'solde']]
    with st.expander(f"Détail des {len(fac)} lignes ouvertes"):
        st.dataframe(fac.rename(columns={
            'piece_ref': 'Réf.', 'ecriture_date': 'Date', 'ecriture_lib': 'Libellé',
            'debit': 'Débit', 'credit': 'Crédit', 'solde': 'Solde'
        }), use_container_width=True, hide_index=True)

    st.subheader("Historique des relances")
    client_notes = notes_df[notes_df['comp_aux_num'] == comp_aux_num] \
        if not notes_df.empty else pd.DataFrame()
    if not client_notes.empty:
        client_notes = client_notes.sort_values('date_note', ascending=False)

    if client_notes.empty:
        st.info("Aucune note pour ce client.")
    else:
        for _, n in client_notes.iterrows():
            icon = {'Ouvert': '🔴', 'En cours': '🟡', 'Résolu': '🟢'}.get(n['statut'], '⚪')
            with st.expander(f"{icon} {n['date_note']} — {n['auteur']} — {n['action'] or '(note)'}"):
                if n.get('note_resume'):
                    st.markdown(f"📌 **Résumé :** {n['note_resume']}")
                st.write(n['note'])
                if n['echeance']:
                    st.caption(f"📅 Échéance : {n['echeance']}")
                cols = st.columns([2, 1, 1])
                statuts = ['Ouvert', 'En cours', 'Résolu']
                cur_idx = statuts.index(n['statut']) if n['statut'] in statuts else 0
                new_st = cols[0].selectbox("Statut", statuts, index=cur_idx,
                                           key=f"st_{n['id']}")
                if cols[1].button("Mettre à jour", key=f"up_{n['id']}"):
                    update_cell_by_id('notes', int(n['id']), 'statut', new_st)
                    st.rerun()
                if cols[2].button("🗑 Supprimer", key=f"del_{n['id']}"):
                    delete_row_by_id('notes', int(n['id']))
                    st.rerun()

    st.subheader("➕ Ajouter une note")
    with st.form("new_note", clear_on_submit=True):
        c1, c2 = st.columns(2)
        auteur = c1.text_input("Auteur", value=st.session_state.get('last_auteur', ''))
        action = c2.text_input("Type d'action",
                               placeholder="ex: Appel, Mail, Relance 1...")
        note = st.text_area("Note détaillée", height=100)
        note_resume = st.text_input(
            "Résumé pour direction (max 100 caractères)",
            max_chars=100,
            placeholder="Synthèse 1 ligne pour reporting direction"
        )
        c3, c4 = st.columns(2)
        echeance = c3.date_input("Échéance (optionnel)", value=None)
        statut = c4.selectbox("Statut", ['Ouvert', 'En cours', 'Résolu'])
        if st.form_submit_button("Enregistrer", type="primary"):
            if note.strip():
                st.session_state['last_auteur'] = auteur
                new_id = next_id(notes_df)
                append_row('notes', {
                    'id': new_id,
                    'ref_client': info.get('ref_client', ''),
                    'comp_aux_num': comp_aux_num,
                    'date_note': datetime.now().strftime('%Y-%m-%d %H:%M'),
                    'auteur': auteur,
                    'note': note,
                    'note_resume': note_resume.strip()[:100],
                    'action': action,
                    'echeance': echeance.isoformat() if echeance else '',
                    'statut': statut,
                })
                st.success("Note enregistrée.")
                st.rerun()


def _style_header(cell):
    cell.font = Font(name='Arial', bold=True, color='FFFFFF', size=11)
    cell.fill = PatternFill('solid', start_color='1F3864')
    cell.alignment = Alignment(horizontal='center', vertical='center')


def _autosize(ws):
    for col_cells in ws.columns:
        length = max((len(str(c.value or '')) for c in col_cells), default=10)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = \
            min(max(length + 2, 12), 40)


def page_export():
    st.header("📤 Export")

    df = load_creances_enrichies(only_open=True)
    df = df[df['solde'] > 0.01]
    if df.empty:
        st.warning("Aucune créance à exporter.")
        return

    notes = read_sheet('notes')
    if not notes.empty:
        last_notes = notes.groupby('comp_aux_num').agg(
            derniere_relance=('date_note', 'max'),
            nb_relances=('date_note', 'count')
        ).reset_index()
    else:
        last_notes = pd.DataFrame(columns=['comp_aux_num', 'derniere_relance', 'nb_relances'])

    # Sépare contentieux avant exports
    df_all = df.copy()
    df_ctx = df_all[df_all.get('contentieux', False)] if 'contentieux' in df_all.columns \
        else pd.DataFrame()
    df = df_all[~df_all.get('contentieux', False)] if 'contentieux' in df_all.columns \
        else df_all

    tab1, tab2, tab3, tab4 = st.tabs(["Export commerciaux", "Export Power BI",
                                       "Export Contentieux", "Export Direction"])

    with tab1:
        st.markdown("Classeur Excel avec synthèse + une feuille par commercial + relances.")
        if not df_ctx.empty:
            st.caption(f"ℹ️ {df_ctx['ref_client'].nunique()} dossier(s) en contentieux "
                       f"exclu(s) de cet export.")
        if st.button("🔧 Générer l'export commerciaux", type="primary"):
            wb = openpyxl.Workbook()
            wb.remove(wb.active)

            ws = wb.create_sheet("Synthèse")
            headers = ['Commercial', 'Agence', 'Client FEC', 'Ref dossier', 'Client CRM',
                       'État', 'Solde dû (€)', 'Nb factures', 'Dernière relance', 'Nb relances']
            ws.append(headers)
            for c in ws[1]:
                _style_header(c)

            synth = df.merge(last_notes, on='comp_aux_num', how='left')
            synth = synth.groupby(['commercial', 'agence', 'comp_aux_lib', 'ref_client',
                                   'client', 'etat'], dropna=False).agg(
                solde=('solde', 'sum'),
                nb=('piece_ref', 'count'),
                derniere_relance=('derniere_relance', 'max'),
                nb_relances=('nb_relances', 'max')
            ).reset_index().sort_values('solde', ascending=False)

            for _, r in synth.iterrows():
                ws.append([r['commercial'], r['agence'], r['comp_aux_lib'],
                           r['ref_client'], r['client'], r['etat'],
                           round(r['solde'], 2), r['nb'],
                           r['derniere_relance'], r['nb_relances'] or 0])

            total_row = ws.max_row + 1
            ws.cell(total_row, 1, 'TOTAL').font = Font(bold=True)
            ws.cell(total_row, 7, f'=SUM(G2:G{total_row - 1})').font = Font(bold=True)
            for row in ws.iter_rows(min_row=2, max_row=total_row, min_col=7, max_col=7):
                for c in row:
                    c.number_format = '#,##0.00 €'
            _autosize(ws)
            ws.freeze_panes = 'A2'

            for com in sorted(df['commercial'].dropna().unique()):
                if not com:
                    continue
                df_c = df[df['commercial'] == com].sort_values(['comp_aux_lib', 'ecriture_date'])
                safe = com[:31].replace('/', '-').replace('\\', '-')
                ws = wb.create_sheet(safe)
                headers = ['Client', 'Ref dossier', 'Réf. pièce', 'Date', 'Journal',
                           'Libellé', 'Débit', 'Crédit', 'Solde', 'Agence', 'État']
                ws.append(headers)
                for c in ws[1]:
                    _style_header(c)
                for _, r in df_c.iterrows():
                    ws.append([r['comp_aux_lib'], r['ref_client'], r['piece_ref'],
                               r['ecriture_date'], r['journal_code'], r['ecriture_lib'],
                               round(r['debit'], 2), round(r['credit'], 2),
                               round(r['solde'], 2), r['agence'], r['etat']])
                last = ws.max_row + 1
                ws.cell(last, 1, 'TOTAL').font = Font(bold=True)
                ws.cell(last, 9, f'=SUM(I2:I{last - 1})').font = Font(bold=True)
                for row in ws.iter_rows(min_row=2, max_row=last, min_col=7, max_col=9):
                    for c in row:
                        c.number_format = '#,##0.00 €'
                _autosize(ws)
                ws.freeze_panes = 'A2'

            non_map = df[df['commercial'].fillna('') == '']
            if not non_map.empty:
                ws = wb.create_sheet("Non rattachés")
                ws.append(['Client FEC', 'Code compta', 'Réf. pièce', 'Date',
                           'Libellé', 'Solde'])
                for c in ws[1]:
                    _style_header(c)
                for _, r in non_map.iterrows():
                    ws.append([r['comp_aux_lib'], r['comp_aux_num'], r['piece_ref'],
                               r['ecriture_date'], r['ecriture_lib'], round(r['solde'], 2)])
                _autosize(ws)

            if not notes.empty:
                ws = wb.create_sheet("Relances")
                ws.append(['Date', 'Client', 'Auteur', 'Action', 'Note', 'Échéance', 'Statut'])
                for c in ws[1]:
                    _style_header(c)
                for _, r in notes.iterrows():
                    ws.append([r['date_note'], r['comp_aux_num'], r['auteur'],
                               r['action'], r['note'], r['echeance'], r['statut']])
                _autosize(ws)
                ws.freeze_panes = 'A2'

            buf = io.BytesIO()
            wb.save(buf)
            st.download_button(
                "📥 Télécharger (relances_commerciaux.xlsx)",
                data=buf.getvalue(),
                file_name=f"relances_commerciaux_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    with tab2:
        st.markdown("Dataset plat pour Power BI avec tranches d'âge et dernière relance.")
        if st.button("🔧 Générer l'export Power BI", type="primary"):
            pbi = df.copy()
            pbi['ecriture_date'] = pd.to_datetime(pbi['ecriture_date'], errors='coerce')
            pbi['annee'] = pbi['ecriture_date'].dt.year
            pbi['mois'] = pbi['ecriture_date'].dt.to_period('M').astype(str)
            pbi['age_jours'] = (pd.Timestamp.now().normalize() - pbi['ecriture_date']).dt.days

            def tranche(j):
                if pd.isna(j): return 'N/A'
                if j <= 30: return '0-30 j'
                if j <= 60: return '31-60 j'
                if j <= 90: return '61-90 j'
                if j <= 180: return '91-180 j'
                return '> 180 j'

            pbi['tranche_age'] = pbi['age_jours'].apply(tranche)
            pbi = pbi.merge(last_notes, on='comp_aux_num', how='left')

            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as wr:
                pbi.to_excel(wr, index=False, sheet_name='Creances')
                read_sheet('dossiers').to_excel(wr, index=False, sheet_name='Dossiers')
                read_sheet('notes').to_excel(wr, index=False, sheet_name='Notes')
            st.download_button(
                "📥 Télécharger (export_powerbi.xlsx)",
                data=out.getvalue(),
                file_name=f"creances_powerbi_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.caption("💡 Power BI : Obtenir les données → Excel → charger les 3 feuilles.")

    with tab3:
        st.markdown("Export des dossiers en contentieux groupés par responsable.")
        if df_ctx.empty:
            st.info("Aucun dossier en contentieux. Ajoutez-les dans "
                    "Import → Contentieux.")
        else:
            st.caption(f"{df_ctx['ref_client'].nunique()} dossier(s) — "
                       f"total {df_ctx['solde'].sum():,.0f} €".replace(",", " "))
            if st.button("🔧 Générer l'export contentieux", type="primary"):
                wb = openpyxl.Workbook()
                wb.remove(wb.active)

                # Feuille de synthèse
                ws = wb.create_sheet("Synthèse")
                headers = ['Responsable', 'Client FEC', 'Ref dossier', 'Client CRM',
                           'Commercial', 'Agence', 'Solde (€)', 'Nb factures',
                           'Jours retard max', 'Dernière écriture']
                ws.append(headers)
                for c in ws[1]:
                    _style_header(c)

                synth_ct = df_ctx.groupby(
                    ['responsable', 'comp_aux_lib', 'ref_client', 'client',
                     'commercial', 'agence'], dropna=False).agg(
                    solde=('solde', 'sum'),
                    nb=('piece_ref', 'count'),
                    jours=('jours_retard', 'max'),
                    derniere=('ecriture_date', 'max')
                ).reset_index().sort_values(['responsable', 'solde'],
                                             ascending=[True, False])

                for _, r in synth_ct.iterrows():
                    ws.append([r['responsable'], r['comp_aux_lib'], r['ref_client'],
                               r['client'], r['commercial'], r['agence'],
                               round(r['solde'], 2), r['nb'],
                               int(r['jours']) if pd.notna(r['jours']) else '',
                               r['derniere']])

                total_row = ws.max_row + 1
                ws.cell(total_row, 1, 'TOTAL').font = Font(bold=True)
                ws.cell(total_row, 7, f'=SUM(G2:G{total_row - 1})').font = Font(bold=True)
                for row in ws.iter_rows(min_row=2, max_row=total_row,
                                         min_col=7, max_col=7):
                    for c in row:
                        c.number_format = '#,##0.00 €'
                _autosize(ws)
                ws.freeze_panes = 'A2'

                # Une feuille par responsable
                for resp in sorted(df_ctx['responsable'].dropna().unique()):
                    if not resp:
                        continue
                    df_r = df_ctx[df_ctx['responsable'] == resp] \
                        .sort_values(['comp_aux_lib', 'ecriture_date'])
                    safe = resp[:31].replace('/', '-').replace('\\', '-')
                    ws = wb.create_sheet(safe)
                    headers = ['Client', 'Ref dossier', 'Réf. pièce', 'Date',
                               'Journal', 'Libellé', 'Débit', 'Crédit', 'Solde',
                               'Jours retard', 'Commercial', 'Agence']
                    ws.append(headers)
                    for c in ws[1]:
                        _style_header(c)
                    for _, r in df_r.iterrows():
                        ws.append([r['comp_aux_lib'], r['ref_client'], r['piece_ref'],
                                   r['ecriture_date'], r['journal_code'],
                                   r['ecriture_lib'],
                                   round(r['debit'], 2), round(r['credit'], 2),
                                   round(r['solde'], 2),
                                   int(r.get('jours_retard', 0) or 0),
                                   r['commercial'], r['agence']])
                    last = ws.max_row + 1
                    ws.cell(last, 1, 'TOTAL').font = Font(bold=True)
                    ws.cell(last, 9, f'=SUM(I2:I{last - 1})').font = Font(bold=True)
                    for row in ws.iter_rows(min_row=2, max_row=last,
                                             min_col=7, max_col=9):
                        for c in row:
                            c.number_format = '#,##0.00 €'
                    _autosize(ws)
                    ws.freeze_panes = 'A2'

                buf = io.BytesIO()
                wb.save(buf)
                st.download_button(
                    "📥 Télécharger (export_contentieux.xlsx)",
                    data=buf.getvalue(),
                    file_name=f"contentieux_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    with tab4:
        st.markdown("Synthèse pour la direction : un seul tableau avec les "
                    "résumés de note les plus récents.")
        if st.button("🔧 Générer l'export direction", type="primary"):
            wb = openpyxl.Workbook()
            wb.remove(wb.active)

            # Récupère les notes_resume les plus récents par client
            notes_all = read_sheet('notes')
            last_resume = pd.DataFrame(columns=['comp_aux_num', 'note_resume',
                                                 'date_note', 'auteur'])
            if not notes_all.empty:
                if 'note_resume' not in notes_all.columns:
                    notes_all['note_resume'] = ''
                # On prend la note la plus récente avec un résumé renseigné
                notes_with_resume = notes_all[
                    notes_all['note_resume'].fillna('').astype(str).str.strip() != '']
                if not notes_with_resume.empty:
                    last_resume = notes_with_resume.sort_values('date_note',
                                                                 ascending=False) \
                        .drop_duplicates('comp_aux_num') \
                        [['comp_aux_num', 'note_resume', 'date_note', 'auteur']]

            # Synthèse globale (créances ouvertes hors Hors CRM)
            df_dir = df_all.copy()  # toutes les créances ouvertes (y.c. contentieux)

            ws = wb.create_sheet("Synthèse Direction")
            headers = ['Code compta', 'Client', 'Ref dossier', 'Commercial',
                       'Conducteur', 'Agence', 'État',
                       'Solde dû (€)', 'Jours retard max', 'Statut',
                       'Prov. risque (€)', 'Prov. créances douteuses (€)',
                       'Dernier résumé', 'Date résumé', 'Auteur résumé']
            ws.append(headers)
            for c in ws[1]:
                _style_header(c)

            synth_d = df_dir.groupby(['comp_aux_num', 'comp_aux_lib', 'ref_client',
                                       'commercial', 'conducteur', 'agence',
                                       'etat'], dropna=False).agg(
                solde=('solde', 'sum'),
                jours=('jours_retard', 'max'),
                contentieux=('contentieux', 'first'),
                provision_risque=('provision_risque', 'first'),
                provision_creances_douteuses=('provision_creances_douteuses',
                                                'first'),
            ).reset_index().sort_values('solde', ascending=False)

            synth_d = synth_d.merge(last_resume, on='comp_aux_num', how='left')

            for _, r in synth_d.iterrows():
                statut = "⚖️ Contentieux" if r['contentieux'] else "Suivi commercial"
                ws.append([
                    r['comp_aux_num'], r['comp_aux_lib'], r['ref_client'],
                    r['commercial'], r['conducteur'], r['agence'], r['etat'],
                    round(r['solde'], 2),
                    int(r['jours']) if pd.notna(r['jours']) else '',
                    statut,
                    round(r['provision_risque'] or 0, 2),
                    round(r['provision_creances_douteuses'] or 0, 2),
                    r.get('note_resume', '') or '',
                    r.get('date_note', '') or '',
                    r.get('auteur', '') or '',
                ])

            total_row = ws.max_row + 1
            ws.cell(total_row, 1, 'TOTAL').font = Font(bold=True)
            ws.cell(total_row, 8, f'=SUM(H2:H{total_row - 1})').font = Font(bold=True)
            ws.cell(total_row, 11, f'=SUM(K2:K{total_row - 1})').font = Font(bold=True)
            ws.cell(total_row, 12, f'=SUM(L2:L{total_row - 1})').font = Font(bold=True)
            for col_idx in (8, 11, 12):
                for row in ws.iter_rows(min_row=2, max_row=total_row,
                                         min_col=col_idx, max_col=col_idx):
                    for c in row:
                        c.number_format = '#,##0.00 €'
            _autosize(ws)
            ws.freeze_panes = 'A2'

            buf = io.BytesIO()
            wb.save(buf)
            st.download_button(
                "📥 Télécharger (export_direction.xlsx)",
                data=buf.getvalue(),
                file_name=f"direction_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )


# ============================================================
# NAVIGATION
# ============================================================
PAGES = {
    "📥 Import": page_import,
    "📊 Créances": page_creances,
    "📝 Notes & Relances": page_notes,
    "📤 Export": page_export,
}

# Check config before any page
ok, msg = check_config()
if not ok:
    st.error(f"❌ Configuration manquante : {msg}")
    st.info("""
    **Pour configurer l'app localement :**

    1. Créez le fichier `.streamlit/secrets.toml` dans le dossier du projet
    2. Ajoutez le contenu de votre `google_credentials.json` au bon format
    3. Voir le fichier `.streamlit/secrets.toml.example` pour le modèle

    **Pour Streamlit Cloud :**
    Configurez les secrets dans l'interface (Settings → Secrets).
    """)
    st.stop()

with st.sidebar:
    st.title("💼 Suivi Créances")
    st.caption("DCA — Suivi clients")
    st.divider()
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()

    # Utilise les créances rapprochées (cohérent avec la page Créances)
    _df_side = load_creances_enrichies(only_open=True)
    if not _df_side.empty:
        _df_side = _df_side[_df_side['solde'].abs() > 0.01]
        st.metric("Clients en créance", _df_side['comp_aux_num'].nunique())
        st.metric("Total dû", f"{_df_side['solde'].sum():,.0f} €".replace(",", " "))

    if st.button("🔄 Rafraîchir les données"):
        clear_cache()
        st.rerun()

PAGES[page]()
