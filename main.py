from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore, storage

cred = credentials.Certificate('./ServiceAccountKey.json')
firebase_app = firebase_admin.initialize_app(cred, {
    'storageBucket': 'wahaapplication.appspot.com'
})

db = firestore.client()
col_ref = db.collection("cloud")

for doc in col_ref.stream():
    doc_dict = doc.to_dict()
    path = doc_dict["path"]
    filecode = path[:path.index("/")]
    try:
        date_string = doc_dict["date"][:19]
    except KeyError:
        date_string = "2000-01-01 00:00:00"

    date = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    if abs((date - datetime.now()).days) > 14:
        print(f'{filecode} is going to get deleted')
        bucket = storage.bucket()
        try:
            bucket.get_blob(path).delete()
        finally:
            col_ref.document(filecode).delete()
