import logging

from flask import Flask, request, jsonify
from config.config import Config
from kafkaHandler.producer import Producer
from dbHandler.db_manager import DbManager
from utils.utils import *
from flasgger import Swagger
from flasgger.utils import swag_from
from swagger_ui import flask_api_doc

app = Flask(__name__)
app.config["SWAGGER"] = {"title":"Swagger-UI", "uiversion":2}
swagger = Swagger(app)

logging.basicConfig(filename='logger.log', filemode='w', 
					format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
					datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger()

kafka_content_obj = Producer(Config.get("KAFKA_SERVERS"), 
								Config.get("REQUEST_MANAGER_TOPIC"))

db_conn_content = DbManager(Config.get("MONGO_DB_URI"), Config.get("MONGO_CONTENT_DB"), Config.get("MONGO_CONTENT_COLLECTION"))


@swag_from("doc/upload_content.yml")
@app.route('/content/upload', methods=['POST'])
def addNewContent():
	req_data = request.get_json(force=True)
	req_data["isContent"] = True
	kafka_content_obj.produce(req_data)

	return jsonify({"status":"Uploading content in progress...."})

@swag_from("doc/upload_user.yml")
@app.route('/user/upload', methods=['POST'])
def addNewUser():
	req_data = request.get_json(force=True)
	req_data["isContent"] = False
	kafka_content_obj.produce(req_data)

	return jsonify({"status":"Uploading user in progress...."})

@swag_from("doc/unlock_chapter.yml")
@app.route('/content/unlock', methods=['POST'])
def unblockChapter():
	req_data = request.get_json(force=True)

	user_id = req_data["user-id"]
	series_id = req_data["series-id"]

	if not unlockChapterForUser(user_id, series_id):
		logger.error("Could not unlock new chapter")
		return jsonify({"status": "unlock unsuccessful"}), 500

	return jsonify({"status": "unlocked successfully"})

@swag_from("doc/getUserDetails.yml")
@app.route('/user/<userId>', methods=['GET'])
def getUserDetails(userId):
	response  = extractUserDetails(userId)

	return jsonify(response)

@swag_from("doc/getContentDetails.yml")
@app.route('/content', methods=['GET'])
def getContentDetails():
	response  = extractContentDetails()

	return jsonify(response)

@swag_from("doc/searchContent.yml")
@app.route('/content/search', methods=['GET'])
def searchContent():
	args = request.args

	search_query = CreateContentSearchQuery(args)

	query_result = runSearchQuery(search_query)

	return jsonify(query_result)



if __name__ == '__main__':
	app.run(host = "0.0.0.0",port=5002, debug=True)