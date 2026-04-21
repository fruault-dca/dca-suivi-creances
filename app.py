"""
Suivi des Créances Clients - Application Streamlit
Import FEC + CRM, suivi, notes, export commerciaux et Power BI
"""
import streamlit as st
import pandas as pd
import sqlite3
import io
from datetime import datetime
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

DB_PATH = Path(__file__).parent / "data" / "suivi.db"
DB_PATH.parent.mkdir(exist_ok=True)

st.set_page_config(page_title="Suivi Créances Clients", page_icon="📊", layout="wide")


# ============================================================
# BASE DE DONNÉES
# ============================================================
def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS creances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_aux_num TEXT,
            comp_aux_lib TEXT,
            piece_ref TEXT,
            ecriture_date TEXT,
            journal_code TEXT,
            ecriture_lib TEXT,
            debit REAL DEFAULT 0,
            credit REAL DEFAULT 0,
            ecriture_let TEXT,
            import_date TEXT
        );
        CREATE TABLE IF NOT EXISTS dossiers (
            ref_client TEXT PRIMARY KEY,
            code_affaire TEXT,
            client TEXT,
            email1 TEXT,
            email2 TEXT,
            type_projet TEXT,
            adresse TEXT,
            cp TEXT,
            ville TEXT,
            constructeur TEXT,
            agence TEXT,
            commercial TEXT,
            etat TEXT,
            stade TEXT,
            type_contrat TEXT,
            contrat_ht REAL,
            contrat_ttc REAL,
            contrat_rev_ht REAL,
            contrat_rev_ttc REAL,
            avenants_ht REAL,
            avenants_ttc REAL,
            date_signature TEXT,
            date_reception TEXT
        );
        CREATE TABLE IF NOT EXISTS mapping (
            comp_aux_num TEXT PRIMARY KEY,
            ref_client TEXT,
            piece_ref TEXT
        );
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref_client TEXT,
            comp_aux_num TEXT,
            date_note TEXT,
            auteur TEXT,
            note TEXT,
            action TEXT,
            echeance TEXT,
            statut TEXT DEFAULT 'Ouvert'
        );
    """)
    conn.commit()
    conn.close()


init_db()


# ============================================================
# HELPERS
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
    """20260101 -> 2026-01-01"""
    s = to_str(d)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


# ============================================================
# PARSERS
# ============================================================
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
        raise ValueError("Impossible de décoder le FEC (encoding)")

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


# ============================================================
# PAGES
# ============================================================
def page_import():
    st.header("📥 Import des données")

    tab1, tab2, tab3 = st.tabs(["FEC", "CRM (Chantiers)", "Mapping factures"])

    with tab1:
        st.markdown("**Import du Fichier d'Écritures Comptables**")
        st.caption("Extrait les comptes 411xxx (créances clients). Les écritures lettrées seront marquées comme soldées.")
        fec_file = st.file_uploader("Fichier FEC (.txt)", type=['txt'], key='fec')
        if fec_file and st.button("Importer le FEC", type="primary"):
            try:
                with st.spinner("Analyse du FEC..."):
                    clients = parse_fec(fec_file.read())
                    conn = get_conn()
                    conn.execute("DELETE FROM creances")
                    rows = [(r['CompAuxNum'], r['CompAuxLib'], r['PieceRef'],
                             format_date_fec(r['EcritureDate']), r['JournalCode'],
                             r['EcritureLib'], r['Debit'], r['Credit'],
                             to_str(r['EcritureLet']), datetime.now().isoformat())
                            for _, r in clients.iterrows()]
                    conn.executemany("""INSERT INTO creances
                        (comp_aux_num, comp_aux_lib, piece_ref, ecriture_date, journal_code,
                         ecriture_lib, debit, credit, ecriture_let, import_date)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""", rows)
                    conn.commit()
                    conn.close()
                st.success(f"✅ {len(clients)} écritures clients importées")
            except Exception as e:
                st.error(f"Erreur: {e}")

    with tab2:
        st.markdown("**Import de l'export CRM (Chantiers)**")
        st.caption("Données des dossiers : ref client, commercial, agence, état, montants contrat.")
        crm_file = st.file_uploader("Export CRM (.xlsx)", type=['xlsx'], key='crm')
        if crm_file:
            try:
                xl = pd.ExcelFile(io.BytesIO(crm_file.getvalue()))
                sheet = st.selectbox("Feuille à importer", xl.sheet_names,
                                     index=xl.sheet_names.index('Liste complète')
                                     if 'Liste complète' in xl.sheet_names else 0)
                if st.button("Importer le CRM", type="primary"):
                    with st.spinner("Analyse du CRM..."):
                        data = parse_crm(crm_file.getvalue(), sheet_name=sheet)
                        conn = get_conn()
                        conn.execute("DELETE FROM dossiers")

                        def g(row, col):
                            return to_str(row.get(col, ''))

                        def gf(row, col):
                            v = row.get(col, '')
                            if pd.isna(v) or str(v).lower() == 'nan' or str(v).strip() == '':
                                return None
                            return to_float(v)

                        rows = []
                        for _, r in data.iterrows():
                            ref = g(r, 'Ref client')
                            if not ref:
                                continue
                            rows.append((
                                ref, g(r, 'N°Compta/Code Affaire'), g(r, 'Client(s)'),
                                g(r, 'Client Email 1'), g(r, 'Client Email 2'),
                                g(r, 'Type de projet'), g(r, 'Adresse du projet'),
                                g(r, 'CP'), g(r, 'Ville'), g(r, 'Constructeur'),
                                g(r, 'Agence'), g(r, 'Commercial'), g(r, 'Etat'),
                                g(r, "Stade d'avancement"), g(r, 'Type de contrat'),
                                gf(r, 'Contrat HT'), gf(r, 'Contrat TTC'),
                                gf(r, 'Contrat révisé HT'), gf(r, 'Contrat révisé TTC'),
                                gf(r, 'Avenants HT'), gf(r, 'Avenants TTC'),
                                g(r, 'Date de signature du contrat'),
                                g(r, 'Date de réception'),
                            ))
                        conn.executemany("""INSERT OR REPLACE INTO dossiers
                            (ref_client, code_affaire, client, email1, email2, type_projet,
                             adresse, cp, ville, constructeur, agence, commercial, etat, stade,
                             type_contrat, contrat_ht, contrat_ttc, contrat_rev_ht, contrat_rev_ttc,
                             avenants_ht, avenants_ttc, date_signature, date_reception)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
                        conn.commit()
                        conn.close()
                    st.success(f"✅ {len(rows)} dossiers importés")
            except Exception as e:
                st.error(f"Erreur: {e}")

    with tab3:
        st.markdown("**Mapping Code comptable ↔ Référence dossier**")
        st.caption("Fichier Excel/CSV reliant `CompAuxNum` (code comptable FEC) à `Ref client` (CRM).")
        st.info("Colonnes attendues : `comp_aux_num` (ex: CROBERT) et `ref_client` (ex: 00655). "
                "Optionnel : `piece_ref` pour un mapping au niveau facture.")

        map_file = st.file_uploader("Mapping (.csv ou .xlsx)", type=['csv', 'xlsx'], key='map')
        if map_file and st.button("Importer le mapping", type="primary"):
            try:
                if map_file.name.endswith('.csv'):
                    df_map = pd.read_csv(map_file, dtype=str)
                else:
                    df_map = pd.read_excel(map_file, dtype=str)
                df_map.columns = [c.strip().lower() for c in df_map.columns]
                conn = get_conn()
                rows = [(to_str(r.get('comp_aux_num', '')),
                         to_str(r.get('ref_client', '')),
                         to_str(r.get('piece_ref', '')))
                        for _, r in df_map.iterrows()
                        if to_str(r.get('comp_aux_num', ''))]
                conn.executemany("INSERT OR REPLACE INTO mapping VALUES (?,?,?)", rows)
                conn.commit()
                conn.close()
                st.success(f"✅ {len(rows)} correspondances importées")
            except Exception as e:
                st.error(f"Erreur : {e}")

        st.markdown("**Mapping manuel** (clients non mappés)")
        conn = get_conn()
        non_mappes = pd.read_sql("""
            SELECT DISTINCT c.comp_aux_num, c.comp_aux_lib,
                ROUND(SUM(c.debit - c.credit), 2) as solde
            FROM creances c
            LEFT JOIN mapping m ON c.comp_aux_num = m.comp_aux_num
            WHERE m.comp_aux_num IS NULL AND c.ecriture_let IS NULL OR c.ecriture_let = ''
            GROUP BY c.comp_aux_num
            HAVING solde > 0
            ORDER BY solde DESC
        """, conn)
        dossiers = pd.read_sql("SELECT ref_client, client FROM dossiers ORDER BY ref_client", conn)
        conn.close()

        if not non_mappes.empty and not dossiers.empty:
            st.write(f"**{len(non_mappes)} clients non mappés avec solde ouvert**")
            options = [''] + [f"{r['ref_client']} - {r['client']}" for _, r in dossiers.iterrows()]
            for _, row in non_mappes.head(20).iterrows():
                c1, c2, c3 = st.columns([2, 2, 1])
                c1.write(f"**{row['comp_aux_lib']}** ({row['comp_aux_num']})")
                sel = c2.selectbox("Ref dossier", options, key=f"map_{row['comp_aux_num']}",
                                   label_visibility="collapsed")
                c3.write(f"{row['solde']:,.0f} €")
                if sel:
                    ref = sel.split(' - ')[0]
                    conn2 = get_conn()
                    conn2.execute("INSERT OR REPLACE INTO mapping VALUES (?,?,?)",
                                  (row['comp_aux_num'], ref, ''))
                    conn2.commit()
                    conn2.close()
                    st.rerun()

    # Stats
    st.divider()
    conn = get_conn()
    n_cr = conn.execute("SELECT COUNT(*) FROM creances").fetchone()[0]
    n_cr_ouv = conn.execute("""SELECT COUNT(*) FROM creances
        WHERE (ecriture_let IS NULL OR ecriture_let = '')""").fetchone()[0]
    n_dos = conn.execute("SELECT COUNT(*) FROM dossiers").fetchone()[0]
    n_map = conn.execute("SELECT COUNT(*) FROM mapping").fetchone()[0]
    conn.close()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Écritures FEC", n_cr)
    c2.metric("Écritures non lettrées", n_cr_ouv)
    c3.metric("Dossiers CRM", n_dos)
    c4.metric("Mappings actifs", n_map)


def load_creances_enrichies(only_open=True):
    conn = get_conn()
    where = "WHERE (c.ecriture_let IS NULL OR c.ecriture_let = '')" if only_open else ""
    df = pd.read_sql(f"""
        SELECT c.id, c.comp_aux_num, c.comp_aux_lib, c.piece_ref, c.ecriture_date,
               c.journal_code, c.ecriture_lib, c.debit, c.credit,
               (c.debit - c.credit) as solde,
               m.ref_client,
               d.client, d.commercial, d.agence, d.etat, d.stade,
               d.contrat_ttc, d.date_reception
        FROM creances c
        LEFT JOIN mapping m ON c.comp_aux_num = m.comp_aux_num
        LEFT JOIN dossiers d ON m.ref_client = d.ref_client
        {where}
        ORDER BY solde DESC
    """, conn)
    conn.close()
    return df


def page_creances():
    st.header("📊 Créances ouvertes")

    df = load_creances_enrichies(only_open=True)
    if df.empty:
        st.warning("Aucune créance. Importez le FEC dans l'onglet Import.")
        return

    df = df[df['solde'].abs() > 0.01]

    with st.container():
        c1, c2, c3, c4 = st.columns(4)
        coms = ['(Tous)'] + sorted([c for c in df['commercial'].dropna().unique() if c])
        filt_com = c1.selectbox("Commercial", coms)
        ags = ['(Toutes)'] + sorted([a for a in df['agence'].dropna().unique() if a])
        filt_ag = c2.selectbox("Agence", ags)
        etats = ['(Tous)'] + sorted([e for e in df['etat'].dropna().unique() if e])
        filt_et = c3.selectbox("État dossier", etats)
        seuil = c4.number_input("Solde mini (€)", value=0, step=500)

    f = df.copy()
    if filt_com != '(Tous)':
        f = f[f['commercial'] == filt_com]
    if filt_ag != '(Toutes)':
        f = f[f['agence'] == filt_ag]
    if filt_et != '(Tous)':
        f = f[f['etat'] == filt_et]
    f = f[f['solde'] >= seuil]

    total = f['solde'].sum()
    nb_cli = f['comp_aux_num'].nunique()
    nb_dos_mappes = f['ref_client'].notna().sum()
    non_mappe = f[f['ref_client'].isna()]['solde'].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Solde total dû", f"{total:,.0f} €".replace(",", " "))
    k2.metric("Clients concernés", nb_cli)
    k3.metric("Lignes rattachées CRM", f"{nb_dos_mappes} / {len(f)}")
    k4.metric("Non rattaché (€)", f"{non_mappe:,.0f}".replace(",", " "))

    st.subheader("Synthèse par client")
    synth = f.groupby(['comp_aux_num', 'comp_aux_lib', 'ref_client', 'client',
                       'commercial', 'agence', 'etat'], dropna=False).agg(
        solde=('solde', 'sum'),
        nb=('piece_ref', 'count'),
        derniere_ecriture=('ecriture_date', 'max')
    ).reset_index().sort_values('solde', ascending=False)

    st.dataframe(
        synth.rename(columns={
            'comp_aux_num': 'Code compta', 'comp_aux_lib': 'Client FEC',
            'ref_client': 'Ref dossier', 'client': 'Client CRM',
            'commercial': 'Commercial', 'agence': 'Agence',
            'etat': 'État', 'solde': 'Solde (€)',
            'nb': 'Nb lignes', 'derniere_ecriture': 'Dernière écriture'
        }),
        use_container_width=True, hide_index=True,
        column_config={'Solde (€)': st.column_config.NumberColumn(format="%.2f")}
    )

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

    conn = get_conn()
    clients = pd.read_sql("""
        SELECT DISTINCT c.comp_aux_num, c.comp_aux_lib,
            ROUND(SUM(c.debit - c.credit), 2) as solde,
            d.client, d.commercial
        FROM creances c
        LEFT JOIN mapping m ON c.comp_aux_num = m.comp_aux_num
        LEFT JOIN dossiers d ON m.ref_client = d.ref_client
        WHERE (c.ecriture_let IS NULL OR c.ecriture_let = '')
        GROUP BY c.comp_aux_num
        HAVING solde > 0
        ORDER BY solde DESC
    """, conn)

    if clients.empty:
        st.warning("Aucune créance ouverte. Importez le FEC.")
        conn.close()
        return

    labels = {f"{r['comp_aux_lib']} — {r['solde']:,.0f} €".replace(",", " "): r['comp_aux_num']
              for _, r in clients.iterrows()}
    sel = st.selectbox("Client", ['— Vue globale —'] + list(labels.keys()))

    if sel == '— Vue globale —':
        notes = pd.read_sql("""
            SELECT n.date_note, n.comp_aux_num, d.client, d.commercial,
                   n.auteur, n.note, n.action, n.echeance, n.statut
            FROM notes n
            LEFT JOIN mapping m ON n.comp_aux_num = m.comp_aux_num
            LEFT JOIN dossiers d ON m.ref_client = d.ref_client
            ORDER BY n.date_note DESC
        """, conn)
        conn.close()
        st.subheader(f"Toutes les relances ({len(notes)})")
        if notes.empty:
            st.info("Aucune note enregistrée.")
        else:
            st.dataframe(notes, use_container_width=True, hide_index=True)
        return

    comp_aux_num = labels[sel]
    info = clients[clients['comp_aux_num'] == comp_aux_num].iloc[0]

    c1, c2, c3 = st.columns(3)
    c1.metric("Solde dû", f"{info['solde']:,.2f} €".replace(",", " "))
    c2.write(f"**Client CRM :** {info['client'] or '(non rattaché)'}")
    c3.write(f"**Commercial :** {info['commercial'] or '—'}")

    fac = pd.read_sql("""SELECT piece_ref, ecriture_date, ecriture_lib, debit, credit,
        (debit-credit) as solde FROM creances
        WHERE comp_aux_num = ? AND (ecriture_let IS NULL OR ecriture_let = '')
        ORDER BY ecriture_date""", conn, params=(comp_aux_num,))
    with st.expander(f"Détail des {len(fac)} lignes ouvertes", expanded=False):
        st.dataframe(fac.rename(columns={
            'piece_ref': 'Réf.', 'ecriture_date': 'Date', 'ecriture_lib': 'Libellé',
            'debit': 'Débit', 'credit': 'Crédit', 'solde': 'Solde'
        }), use_container_width=True, hide_index=True)

    st.subheader("Historique des relances")
    notes = pd.read_sql("SELECT * FROM notes WHERE comp_aux_num=? ORDER BY date_note DESC",
                        conn, params=(comp_aux_num,))
    conn.close()

    if notes.empty:
        st.info("Aucune note pour ce client.")
    else:
        for _, n in notes.iterrows():
            icon = {'Ouvert': '🔴', 'En cours': '🟡', 'Résolu': '🟢'}.get(n['statut'], '⚪')
            with st.expander(f"{icon} {n['date_note']} — {n['auteur']} — {n['action'] or '(note)'}",
                             expanded=False):
                st.write(n['note'])
                if n['echeance']:
                    st.caption(f"📅 Échéance : {n['echeance']}")
                cols = st.columns([2, 1, 1])
                new_st = cols[0].selectbox(
                    "Statut", ['Ouvert', 'En cours', 'Résolu'],
                    index=['Ouvert', 'En cours', 'Résolu'].index(n['statut']),
                    key=f"st_{n['id']}"
                )
                if cols[1].button("Mettre à jour", key=f"up_{n['id']}"):
                    c2 = get_conn()
                    c2.execute("UPDATE notes SET statut=? WHERE id=?", (new_st, n['id']))
                    c2.commit()
                    c2.close()
                    st.rerun()
                if cols[2].button("🗑 Supprimer", key=f"del_{n['id']}"):
                    c2 = get_conn()
                    c2.execute("DELETE FROM notes WHERE id=?", (n['id'],))
                    c2.commit()
                    c2.close()
                    st.rerun()

    st.subheader("➕ Ajouter une note")
    with st.form("new_note", clear_on_submit=True):
        c1, c2 = st.columns(2)
        auteur = c1.text_input("Auteur", value=st.session_state.get('last_auteur', ''))
        action = c2.text_input("Type d'action", placeholder="ex: Appel, Mail, Relance 1...")
        note = st.text_area("Note", height=100)
        c3, c4 = st.columns(2)
        echeance = c3.date_input("Échéance (optionnel)", value=None)
        statut = c4.selectbox("Statut", ['Ouvert', 'En cours', 'Résolu'])
        if st.form_submit_button("Enregistrer", type="primary"):
            if note.strip():
                st.session_state['last_auteur'] = auteur
                c = get_conn()
                c.execute("""INSERT INTO notes
                    (comp_aux_num, date_note, auteur, note, action, echeance, statut)
                    VALUES (?,?,?,?,?,?,?)""",
                          (comp_aux_num, datetime.now().strftime('%Y-%m-%d %H:%M'),
                           auteur, note, action,
                           echeance.isoformat() if echeance else None, statut))
                c.commit()
                c.close()
                st.success("Note enregistrée.")
                st.rerun()


def _style_header(cell):
    cell.font = Font(name='Arial', bold=True, color='FFFFFF', size=11)
    cell.fill = PatternFill('solid', start_color='1F3864')
    cell.alignment = Alignment(horizontal='center', vertical='center')


def _autosize(ws):
    for col_cells in ws.columns:
        length = max((len(str(c.value or '')) for c in col_cells), default=10)
        ws.column_dimensions[get_column_letter(col_cells[0].column)].width = min(max(length + 2, 12), 40)


def page_export():
    st.header("📤 Export")

    df = load_creances_enrichies(only_open=True)
    df = df[df['solde'] > 0.01]

    if df.empty:
        st.warning("Aucune créance à exporter.")
        return

    conn = get_conn()
    notes = pd.read_sql("""
        SELECT n.*, d.client, d.commercial, d.agence
        FROM notes n
        LEFT JOIN mapping m ON n.comp_aux_num = m.comp_aux_num
        LEFT JOIN dossiers d ON m.ref_client = d.ref_client
        ORDER BY n.date_note DESC
    """, conn)
    last_notes = pd.read_sql("""
        SELECT comp_aux_num,
               MAX(date_note) as derniere_relance,
               COUNT(*) as nb_relances
        FROM notes GROUP BY comp_aux_num
    """, conn)
    conn.close()

    tab1, tab2 = st.tabs(["Export commerciaux", "Export Power BI"])

    with tab1:
        st.markdown("Génère un classeur Excel avec une feuille par commercial, "
                    "synthèse globale + historique des relances.")
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
            ws.cell(total_row, 7).number_format = '#,##0.00 €'
            for row in ws.iter_rows(min_row=2, max_row=total_row, min_col=7, max_col=7):
                for c in row:
                    c.number_format = '#,##0.00 €'
            _autosize(ws)
            ws.freeze_panes = 'A2'

            for com in sorted(df['commercial'].dropna().unique()):
                df_c = df[df['commercial'] == com].sort_values(
                    ['comp_aux_lib', 'ecriture_date'])
                safe_name = com[:31].replace('/', '-').replace('\\', '-')
                ws = wb.create_sheet(safe_name)
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

            non_map = df[df['commercial'].isna() | (df['commercial'] == '')]
            if not non_map.empty:
                ws = wb.create_sheet("Non rattachés")
                headers = ['Client FEC', 'Code compta', 'Réf. pièce', 'Date',
                           'Libellé', 'Solde']
                ws.append(headers)
                for c in ws[1]:
                    _style_header(c)
                for _, r in non_map.iterrows():
                    ws.append([r['comp_aux_lib'], r['comp_aux_num'], r['piece_ref'],
                               r['ecriture_date'], r['ecriture_lib'], round(r['solde'], 2)])
                _autosize(ws)

            if not notes.empty:
                ws = wb.create_sheet("Relances")
                headers = ['Date', 'Client', 'Commercial', 'Agence', 'Auteur',
                           'Action', 'Note', 'Échéance', 'Statut']
                ws.append(headers)
                for c in ws[1]:
                    _style_header(c)
                for _, r in notes.iterrows():
                    ws.append([r['date_note'], r['client'] or r['comp_aux_num'],
                               r['commercial'], r['agence'], r['auteur'],
                               r['action'], r['note'], r['echeance'], r['statut']])
                _autosize(ws)
                ws.freeze_panes = 'A2'

            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            st.download_button(
                "📥 Télécharger (relances_commerciaux.xlsx)",
                data=buf.getvalue(),
                file_name=f"relances_commerciaux_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    with tab2:
        st.markdown("Dataset plat pour Power BI : une ligne par écriture, enrichie avec les infos dossier.")
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
                conn = get_conn()
                pd.read_sql("SELECT * FROM dossiers", conn).to_excel(
                    wr, index=False, sheet_name='Dossiers')
                pd.read_sql("SELECT * FROM notes", conn).to_excel(
                    wr, index=False, sheet_name='Notes')
                conn.close()
            out.seek(0)
            st.download_button(
                "📥 Télécharger (export_powerbi.xlsx)",
                data=out.getvalue(),
                file_name=f"creances_powerbi_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.caption("💡 Dans Power BI : Obtenir les données → Excel → charger les 3 feuilles. "
                       "Relations : Creances.comp_aux_num ↔ Notes.comp_aux_num, "
                       "Creances.ref_client ↔ Dossiers.ref_client")


# ============================================================
# NAVIGATION
# ============================================================
PAGES = {
    "📥 Import": page_import,
    "📊 Créances": page_creances,
    "📝 Notes & Relances": page_notes,
    "📤 Export": page_export,
}

with st.sidebar:
    st.title("💼 Suivi Créances")
    st.caption("DCA — Suivi clients")
    st.divider()
    page = st.radio("Navigation", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()
    conn = get_conn()
    n_ouv = conn.execute("""SELECT COUNT(DISTINCT comp_aux_num) FROM creances
        WHERE (ecriture_let IS NULL OR ecriture_let = '')
        AND (debit - credit) > 0""").fetchone()[0]
    total_ouv = conn.execute("""SELECT COALESCE(SUM(debit - credit), 0) FROM creances
        WHERE (ecriture_let IS NULL OR ecriture_let = '')""").fetchone()[0] or 0
    conn.close()
    st.metric("Clients en créance", n_ouv)
    st.metric("Total dû", f"{total_ouv:,.0f} €".replace(",", " "))

PAGES[page]()
