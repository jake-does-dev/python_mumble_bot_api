from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from bson import ObjectId
from flask import Flask, request, jsonify, _request_ctx_stack
from flask_cors import cross_origin
from marshmallow import Schema, fields
from api import mongodb
from six.moves.urllib.request import urlopen
from functools import wraps
from jose import jwt

import json

# Get config
config = open('auth_config.json', 'r').read()
config = json.loads(config)

AUTH0_DOMAIN = config["domain"]
API_AUDIENCE = config["audience"]
ALGORITHMS = ["RS256"]

# Create an APISpec
spec = APISpec(
    title="Mumble Bot Flask DB API",
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
# Error Handler
class AuthError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code

# Format error response and append status code
def get_token_auth_header():
    """Obtains the Access Token from the Authorization Header
    """
    auth = request.headers.get("Authorization", None)
    if not auth:
        raise AuthError({"code": "authorization_header_missing",
                        "description":
                            "Authorization header is expected"}, 401)

    parts = auth.split()

    if parts[0].lower() != "bearer":
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must start with"
                            " Bearer"}, 401)
    elif len(parts) == 1:
        raise AuthError({"code": "invalid_header",
                        "description": "Token not found"}, 401)
    elif len(parts) > 2:
        raise AuthError({"code": "invalid_header",
                        "description":
                            "Authorization header must be"
                            " Bearer token"}, 401)

    token = parts[1]
    return token

def requires_auth(f):
    """Determines if the Access Token is valid
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_auth_header()
        jsonurl = urlopen("https://"+AUTH0_DOMAIN+"/.well-known/jwks.json")
        jwks = json.loads(jsonurl.read())
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]
                }
        if rsa_key:
            try:
                payload = jwt.decode(
                    token,
                    rsa_key,
                    algorithms=ALGORITHMS,
                    audience=API_AUDIENCE,
                    issuer="https://"+AUTH0_DOMAIN+"/"
                )
            except jwt.ExpiredSignatureError:
                raise AuthError({"code": "token_expired",
                                "description": "token is expired"}, 401)
            except jwt.JWTClaimsError:
                raise AuthError({"code": "invalid_claims",
                                "description":
                                    "incorrect claims,"
                                    "please check the audience and issuer"}, 401)
            except Exception:
                raise AuthError({"code": "invalid_header",
                                "description":
                                    "Unable to parse authentication"
                                    " token."}, 401)

            _request_ctx_stack.top.current_user = payload
            return f(*args, **kwargs)
        raise AuthError({"code": "invalid_header",
                        "description": "Unable to find appropriate key"}, 401)
    return decorated

@app.route("/clips")
@cross_origin(headers=["Content-Type", "Authorization"])
@requires_auth
def get_clips():
    clips = db.get_clips()
    return {"clips": [ClipSchema().dump(clip) for clip in clips]}


@app.errorhandler(AuthError)
def handle_auth_error(ex):
    response = jsonify(ex.error)
    response.status_code = ex.status_code
    return response

# Register the path and the entities within it
with app.test_request_context():
    spec.path(view=get_clips)

