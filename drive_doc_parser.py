import re
import csv
import os
import sys
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/documents.readonly'
]

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=51239)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def select_document(service):
    results = service.files().list(
        q="mimeType='application/vnd.google-apps.document'",
        pageSize=20,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    if not files:
        print("No Google Docs found.")
        sys.exit(1)

    for i, file in enumerate(files):
        print(f"{i}: {file['name']}")

    while True:
        try:
            choice = int(input("\nSelect a document by number: "))
            if 0 <= choice < len(files):
                selected = files[choice]
                print(f"\nSelected: {selected['name']} ({selected['id']})")
                return selected['id']
            else:
                print("Invalid number. Try again.")
        except ValueError:
            print("Please enter a valid number.")

def extract_text_from_doc(doc):
    content = doc.get('body', {}).get('content', [])
    lines = []

    for element in content:
        paragraph = element.get('paragraph')
        if not paragraph:
            continue
        for elem in paragraph.get('elements', []):
            text_run = elem.get('textRun')
            if text_run:
                text = text_run.get('content', '').strip()
                if text:
                    lines.append(text)

    return '\n'.join(lines)

def parse_employees(text):
    lines = text.strip().split('\n')
    employees = []

    for line in lines:
        line = line.strip()
        if not line or '(' not in line or ')' not in line:
            continue

        match = re.match(r'^(.*?)\s*\((.*?)\)$', line)
        if not match:
            continue

        name = match.group(1).strip()
        roles_raw = match.group(2).strip()
        roles = [r.strip() for r in roles_raw.split(',')]

        known_roles = []
        relationships = []
        employed = ''

        for role in roles:
            cleaned = role.lstrip('?').strip()
            if re.search(r'\b(father|mother|son|daughter|spouse|husband|wife|partner)\b', role.lower()):
                relationships.append(role.strip())
            else:
                known_roles.append(cleaned)

            if re.search(r'\bcurrently\b|\bcurrent\b', role.lower()):
                employed = 'yes'

        employees.append({
            'name': name,
            'location': '',
            'employed': employed,
            'roles': known_roles,
            'relationships': relationships
        })

    return employees

def export_to_csv(employees, output_file):
    max_roles = max((len(e['roles']) for e in employees), default=0)
    max_rel = max((len(e['relationships']) for e in employees), default=0)

    headers = ['employee name', 'employee location', 'currently employed in TTI']
    headers += [f'staff role {i+1}' for i in range(max_roles)]
    headers += [f'family relationship {i+1}' for i in range(max_rel)]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for emp in employees:
            row = [emp['name'], emp['location'], emp['employed']]
            row += emp['roles'] + [''] * (max_roles - len(emp['roles']))
            row += emp['relationships'] + [''] * (max_rel - len(emp['relationships']))
            writer.writerow(row)

def main():
    creds = authenticate()
    drive_service = build('drive', 'v3', credentials=creds)
    docs_service = build('docs', 'v1', credentials=creds)

    doc_id = select_document(drive_service)
    doc = docs_service.documents().get(documentId=doc_id).execute()
    raw_text = extract_text_from_doc(doc)

    employees = parse_employees(raw_text)
    output_path = os.path.join(os.getcwd(), 'output.csv')
    export_to_csv(employees, output_path)

    print(f"\n✅ CSV saved to: {output_path}")

if __name__ == '__main__':
    main()