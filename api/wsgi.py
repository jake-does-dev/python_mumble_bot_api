from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from bson import ObjectId
from flask import Flask
from marshmallow import Schema, fields

from api import mongodb

# Create an APISpec
spec = APISpec(
    title="Python Mumble Bot DB API",
    version="1.0.0",
    openapi_version="3.0.3",
    plugins=[FlaskPlugin(), MarshmallowPlugin()],
)

Schema.TYPE_MAPPING[ObjectId] = fields.String

class ClipSchema(Schema):
    _id = fields.String()
    identifier = fields.String()
    name = fields.String()
    file_prefix = fields.String()
    file = fields.String()
    creation_time = fields.Date()
    tags = fields.List(fields.String())

# Optional security scheme support
api_key_scheme = {"type": "apiKey", "in": "header", "name": "X-API-Key"}
spec.components.security_scheme("ApiKeyAuth", api_key_scheme)

# Optional Flask support
app = Flask(__name__)

db = mongodb.MongoInterface()
db.connect()
db.refresh()

@app.route("/clips")
def get_clips():
    clips = db.get_clips()
    return {"clips": [ClipSchema().dump(clip) for clip in clips]}
    
# Register the path and the entities within it
with app.test_request_context():
    spec.path(view=get_clips)