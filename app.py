import datetime
import json
import os
from argparse import ArgumentParser
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import grapat.export
from grapat import config
from grapat.convert import convert, generate_xml, split_text
from grapat.db import db_execute, db_fetch_results

arg_parser = ArgumentParser()
arg_parser.add_argument("--hostname", default="0.0.0.0", type=str, help="REST API hostname")
arg_parser.add_argument("--port", default=8080, type=int, help="REST API port")
arg_parser.add_argument("--reload", action="store_true", help="Reload service on file changes", default=config.DEBUG)
arg_parser.add_argument("--username", help="sets the username for the current session", default=config.USER)
args = arg_parser.parse_args()


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("db", exist_ok=True)
    db_execute("""  CREATE TABLE IF NOT EXISTS results (
                        `username` text ,
                        `annotation_bundle` text , 
                        `sentence` text , 
                        `graph` longtext, 
                        `layout` longtext, 
                        `time` TIMESTAMP
                        );""")

    db_execute("""CREATE TABLE IF NOT EXISTS `annotation_bundles` (
                      `id` varchar(128) DEFAULT NULL,
                      `semantics` varchar(128) DEFAULT NULL,
                      `entity_id` varchar(128) DEFAULT NULL,
                      `sentence_id` int,
                      `segment` TEXT
                    );
                    """)
    os.chmod("db", 0o777)
    os.chmod(os.path.join("db", "grapat.db"), 0o777)
    yield
    # something for shutdown
    grapat.export.export_db()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/resources", tags=["api"])
async def get_resources(request: Request):
    bundles = db_fetch_results("SELECT DISTINCT id FROM annotation_bundles")
    return enumerate(bundles)


@app.get("/resources/{fname}", tags=["api"])
async def get_resource(fname, request: Request):
    annotation_bundle = db_fetch_results(
        'SELECT id, semantics, entity_id, segment FROM annotation_bundles WHERE id=?',
        (fname,))
    data = generate_xml(fname, annotation_bundle[0][2], [i[3] for i in annotation_bundle])
    return Response(content=data, media_type="application/xml")


@app.post("/resources/{fname}", tags=["api"])
async def update_resource_file(fname, request: Request):
    data = dict(await request.form())
    if not data['segments']:
        return {}
    print(data['segments'])


@app.get("/users", tags=["api"])
async def get_resources(request: Request):
    return [{'username': args.username}]


@app.get("/grapat", tags=["api"])
async def load_from_db(bundle_id: str, sentence_id: str, username: str = "Default"):
    """
    Load annotations from DB
    """
    results = db_fetch_results(
        "SELECT graph, time, layout FROM results WHERE username=? AND annotation_bundle=? AND sentence=?",
        (args.username, bundle_id, sentence_id)
    )
    if results:
        results.sort(key=lambda row: row[1], reverse=True)
        graph, _, layout = results[0]
        graph = json.loads(graph)
        layout = json.loads(layout)
    else:
        graph, layout = None, None
    return {
        'graph': graph,
        'layout': layout
    }


# TODO fix: request object raises error
# class GrapatRequest(BaseModel):
#     annotation_bundle: str
#     sentence: str
#     layout: str
#     graph: str
#     annotator: str
#

@app.post("/grapat", tags=["api"])
async def post_grapat(r: Request):
    """
    Save annotations into DB
    """
    data = dict(await r.form())
    if not data['graph']:
        return {}
    db_execute("INSERT INTO results(username, annotation_bundle, sentence, graph, layout, time) "
               "VALUES(?, ?, ?, ?, ?, ?) ;",
               (args.username, data['annotation_bundle'], data['sentence'], data['graph'], data['layout'],
                datetime.datetime.now()),
               commit=True)


@app.post("/grapat/file", tags=["api"])
async def uploade_new_documents(files: list[UploadFile]):
    for file in files:
        contents = await file.read()
        text_id, segments = convert(file.filename, contents.decode())
        for segment in segments:
            db_execute("INSERT INTO annotation_bundles(id, semantics, entity_id, segment) "
                       "VALUES(?, ?, ?, ?) ;",
                       (text_id, 'argumentation', text_id, segment),
                       commit=True)


@app.delete("/grapat/file", tags=["api"])
async def delete_existing_document(r: Request):
    data = dict(await r.form())
    text_id = data['textId']
    db_execute("DELETE FROM results WHERE annotation_bundle=?", (text_id,), commit=True)
    db_execute("DELETE FROM annotation_bundles WHERE id=?", (text_id,), commit=True)


@app.put("/grapat/file", tags=["api"])
async def update_existing_document(r: Request):
    data = dict(await r.form())
    await delete_existing_document(r)
    text_id = data['textId']
    db_execute("DELETE FROM results WHERE annotation_bundle=?", (text_id,), commit=True)
    segments = json.loads(data['segments'])
    for segment in segments:
        db_execute("INSERT INTO annotation_bundles(id, semantics, entity_id, segment) "
                   "VALUES(?, ?, ?, ?) ;",
                   (text_id, 'argumentation', text_id, segment),
                   commit=True)


@app.post("/grapat/export", tags=["api"])
async def export_db(r: Request):
    """
    Export annotations into DB
    """
    grapat.export.export_db()


@app.get("/grapat/export", tags=["api"])
async def export_db(fileId: str):
    """
    Export annotations into DB
    """
    return grapat.export.export_file(fileId)


@app.post("/edu/segment", tags=["api"])
async def segment_text(r: Request):
    """
    Export annotations into DB
    """
    data = dict(await r.form())
    segments = '\n'.join(
        '\n'.join(split_text(par.replace('\n', ' '))) for par in data.get("text", "").split('\n\n'))
    return {'text': segments}


@app.get("/", tags=["templates"], response_class=HTMLResponse)
async def get_main_page(request: Request):
    return templates.TemplateResponse("grapat.html", {"request": request})


@app.get("/files", tags=["templates"], response_class=HTMLResponse)
async def get_files_page(request: Request):
    return templates.TemplateResponse("files.html", {"request": request})


if __name__ == '__main__':
    uvicorn.run("app:app", host=args.hostname, port=args.port, log_level="debug", reload=args.reload)
