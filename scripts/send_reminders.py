#!/usr/bin/env python3
"""
Envoi quotidien de rappels par email aux utilisateurs.

Pour chaque utilisateur actif, parcourt les notes ouvertes qui lui sont
assignées et envoie un récapitulatif des tâches en retard ou à venir.

Variables d'environnement requises (configurées dans GitHub Secrets) :
- GCP_SERVICE_ACCOUNT : JSON du compte de service Google (string)
- SHEET_ID            : ID du Google Sheet de la base
- SMTP_USER           : adresse email d'envoi
- SMTP_PASSWORD       : mot de passe d'application Google (pas le mdp Gmail)
- APP_URL             : URL de l'app Streamlit (optionnel, pour le lien)
- FORCE_SEND          : "1" pour envoyer même sans rappel urgent (debug)
"""
import json
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Seuils (jours)
J_URGENT = 3  # à traiter dans les 3 prochains jours


def get_spreadsheet():
    raw = os.environ["GCP_SERVICE_ACCOUNT"]
    creds_dict = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(os.environ["SHEET_ID"])


def read_sheet(ss, name):
    try:
        ws = ss.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        return pd.DataFrame()
    return pd.DataFrame(ws.get_all_records())


def render_html(nom, en_retard, urgent, a_venir, sans_ech, app_url):
    css = """
    <style>
      body { font-family: 'Segoe UI', sans-serif; color: #2C3E50; }
      h1 { color: #2C3E50; border-bottom: 3px solid #60A020; padding-bottom: 6px; }
      h2 { margin-top: 28px; }
      table { border-collapse: collapse; width: 100%; margin-top: 8px; }
      th { background: #2C3E50; color: white; padding: 8px; text-align: left; font-size: 13px; }
      td { padding: 8px; border-bottom: 1px solid #E0E0E0; font-size: 13px; }
      .badge-retard { background: #F5BEB6; color: #7A1F12; padding: 2px 8px;
                      border-radius: 4px; font-weight: 600; }
      .badge-urgent { background: #F5D7A8; color: #8B5A00; padding: 2px 8px;
                      border-radius: 4px; font-weight: 600; }
      .badge-venir { background: #C0DD97; color: #355A10; padding: 2px 8px;
                     border-radius: 4px; font-weight: 600; }
      .badge-noech { background: #E0E0E0; color: #444; padding: 2px 8px;
                     border-radius: 4px; font-weight: 600; }
      .footer { margin-top: 32px; font-size: 12px; color: #888; }
      .cta { display: inline-block; background: #60A020; color: white;
             padding: 10px 18px; border-radius: 6px; text-decoration: none;
             margin-top: 16px; font-weight: 600; }
    </style>
    """

    def render_table(df, badge_class, badge_text):
        if df.empty:
            return ""
        rows = ""
        for _, n in df.iterrows():
            client = n.get("comp_aux_num", "") or "—"
            action = n.get("action", "") or "Note"
            note_text = (n.get("note", "") or "")[:200]
            ech = n.get("echeance", "") or "—"
            jours = n.get("_jours_avant", "")
            if isinstance(jours, (int, float)) and not pd.isna(jours):
                j = int(jours)
                if j < 0:
                    info = f"En retard de <strong>{abs(j)} jour(s)</strong>"
                elif j == 0:
                    info = "Échéance <strong>aujourd'hui</strong>"
                else:
                    info = f"Dans <strong>{j} jour(s)</strong>"
            else:
                info = ""
            rows += f"""
            <tr>
              <td><strong>{client}</strong></td>
              <td>{action}<br><span style="color:#666">{note_text}</span></td>
              <td>{ech}<br><span class="{badge_class}">{badge_text}</span></td>
              <td>{info}</td>
            </tr>
            """
        return f"""
        <table>
          <thead><tr><th>Client</th><th>Action / Note</th>
                     <th>Échéance</th><th></th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
        """

    cta = (f'<a class="cta" href="{app_url}">Ouvrir l\'application</a>'
           if app_url else "")

    sections = ""
    if not en_retard.empty:
        sections += f"<h2>🔴 En retard ({len(en_retard)})</h2>" + \
            render_table(en_retard, "badge-retard", "En retard")
    if not urgent.empty:
        sections += f"<h2>🟠 À traiter sous {J_URGENT} jours ({len(urgent)})</h2>" + \
            render_table(urgent, "badge-urgent", "Bientôt")
    if not a_venir.empty:
        sections += f"<h2>🟢 À venir ({len(a_venir)})</h2>" + \
            render_table(a_venir, "badge-venir", "À venir")
    if not sans_ech.empty:
        sections += f"<h2>⚪ Sans échéance ({len(sans_ech)})</h2>" + \
            render_table(sans_ech, "badge-noech", "Sans date")

    return f"""
    <html><head>{css}</head>
    <body>
      <h1>Bonjour {nom}</h1>
      <p>Voici vos tâches de suivi des créances à traiter :</p>
      {sections}
      {cta}
      <div class="footer">
        Email automatique — Suivi des créances clients DCA<br>
        Vous pouvez gérer vos notes directement dans l'application.
      </div>
    </body></html>
    """


def send_email(to_email, to_nom, subject, html, smtp_user, smtp_pwd):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"DCA Suivi Créances <{smtp_user}>"
    msg["To"] = to_email
    msg.attach(MIMEText("Version HTML requise pour ce rappel.", "plain"))
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(smtp_user, smtp_pwd)
        server.sendmail(smtp_user, [to_email], msg.as_string())


def main():
    ss = get_spreadsheet()
    notes = read_sheet(ss, "notes")
    users = read_sheet(ss, "users")

    if notes.empty:
        print("Aucune note à traiter.")
        return
    if users.empty:
        print("Aucun utilisateur enregistré.")
        return

    # Normalise (crée les colonnes manquantes le cas échéant)
    for col, default in [("statut", "Ouvert"), ("assigne_a", ""),
                          ("echeance", ""), ("auteur", ""),
                          ("comp_aux_num", ""), ("action", ""),
                          ("note", ""), ("date_note", "")]:
        if col not in notes.columns:
            notes[col] = default
        notes[col] = notes[col].fillna(default).astype(str)
    notes = notes[notes["statut"] != "Résolu"]

    today = pd.Timestamp(datetime.now().date())
    notes["_ech"] = pd.to_datetime(notes["echeance"], errors="coerce", dayfirst=True)
    notes["_jours_avant"] = (notes["_ech"] - today).dt.days

    smtp_user = os.environ["SMTP_USER"]
    smtp_pwd = os.environ["SMTP_PASSWORD"]
    app_url = os.environ.get("APP_URL", "")
    force = os.environ.get("FORCE_SEND", "0") == "1"

    nb_envois = 0
    for _, u in users.iterrows():
        if str(u.get("actif", "oui")).lower() not in ("oui", "true", "1", "yes", ""):
            continue
        email = str(u.get("email", "")).strip()
        nom = str(u.get("nom_affichage", email)).strip() or email
        if not email or "@" not in email:
            continue

        mes_notes = notes[notes["assigne_a"].str.lower() == email.lower()]
        if mes_notes.empty:
            print(f"[skip] {email} : aucune tâche assignée.")
            continue

        en_retard = mes_notes[mes_notes["_jours_avant"] < 0]
        urgent = mes_notes[(mes_notes["_jours_avant"] >= 0)
                            & (mes_notes["_jours_avant"] <= J_URGENT)]
        a_venir = mes_notes[mes_notes["_jours_avant"] > J_URGENT]
        sans_ech = mes_notes[mes_notes["_ech"].isna()]

        # Envoi à tout utilisateur ayant au moins une tâche ouverte
        # (retard, urgent, à venir, ou sans échéance)
        if mes_notes.empty:
            print(f"[skip] {email} : aucune tâche ouverte.")
            continue

        subject_parts = []
        if not en_retard.empty:
            subject_parts.append(f"{len(en_retard)} en retard")
        if not urgent.empty:
            subject_parts.append(f"{len(urgent)} sous {J_URGENT} jours")
        if not subject_parts:
            subject_parts.append(f"{len(mes_notes)} tâche(s) en cours")
        subject = "[Suivi créances] " + " · ".join(subject_parts)

        html = render_html(nom, en_retard, urgent, a_venir, sans_ech, app_url)
        try:
            send_email(email, nom, subject, html, smtp_user, smtp_pwd)
            print(f"[ok] envoyé à {email} ({len(en_retard)} retard, {len(urgent)} urgent)")
            nb_envois += 1
        except Exception as e:
            print(f"[err] échec pour {email} : {e}")

    print(f"\nTerminé. {nb_envois} email(s) envoyé(s).")


if __name__ == "__main__":
    main()
