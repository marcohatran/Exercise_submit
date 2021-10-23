from fastapi import FastAPI, File, UploadFile
from services.yearly_service import *
from services.enrichment import *
from services.top import *
from threading import Thread
app = FastAPI()


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...)):
    try:
        df = to_df(file)
        df = process_data(df)
        # yearly = run('university', 'yearly', df, file.filename)
        # insert_top_value('university', 'top_university', df, file.filename)
        t1 = Thread(target=run, args=('university', 'yearly', df, file.filename,))
        t2 = Thread(target=insert_enrich_info, args=('university', 'universities', df, file.filename,))
        t3 = Thread(target=insert_top_value, args=('university', 'top_university', df, file.filename,))
        t2.start()
        t1.start()
        t3.start()
        t2.join()
        t1.join()
        t3.join()
        return {"status": 1}
    except Exception as e:
        return {'status': 0,
                'error': str(e)}