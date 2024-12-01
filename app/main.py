from flask import Flask
from datetime import timedelta
from waitress import serve
from app.auth.controller import auth_bp
from app.content.controller import content_bp
from dotenv import load_dotenv
from flask_jwt_extended import JWTManager
import os
# from flasgger import Swagger


load_dotenv()

# JWT_SECRET_KEY = "5zwMNKZym9hl1vS4LwJvQ5NCbVFxKT1shFBXjEXXoHM"

app = Flask(__name__)
app.config['DEBUG'] = True


# Dependencies for encoding and decoding JWT
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)


# Registering blueprints (or routers)
app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(content_bp, url_prefix="/content")


jwt_manager = JWTManager(app)




if __name__ == "__main__":
        # app.run(debug=True)
    serve(app, host='127.0.0.1', port=5000)


