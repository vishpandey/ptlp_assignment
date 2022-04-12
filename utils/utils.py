import logging

from dbHandler.db_manager import DbManager
from config.config import Config
from kafka import KafkaConsumer
from kafkaHandler.producer import Producer
from utils import logger


def getPropValue(prop):
    return Config.get(prop)

def getKafkaProducer(topic):
    return Producer(getPropValue("KAFKA_SERVERS"), getPropValue(topic))

def getKafkaConsumer(consumer_topic):
    return KafkaConsumer(getPropValue(consumer_topic),
                        group_id=getPropValue("KAFKA_CONSUMER_GROUP"),
                        bootstrap_servers=getPropValue("KAFKA_SERVERS"),
                        auto_offset_reset="latest")

def validateUploadRequest(data):

    if data["isContent"] and "series-list" in data and  len(data["series-list"]) >= 1:
        return True

    if not data["isContent"] and "users-list" in data and len(data["users-list"]) >= 1:
        return True

    return False

def getProducerTopic(isContent):
    if isContent:
        return "BULK_CONTENT_UPLOAD_TOPIC"

    return "BULK_USER_UPLOAD_TOPIC"

def CreateContentSearchQuery(args):
	search_query = {}
	
	if args.get("series-title") is not None:
		search_query["series.name"] = args.get("series-title")
	
	if args.get("episode") is not None:
		search_query["chapters.content-metadata.title"] = args.get("episode")

	return search_query

def runSearchQuery(search_query):
	try:
		db_conn_content = getdbConnectionToCollection("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")
		search_results = db_conn_content.findAll(search_query)
	except Exception as e:
		logger.error(e)
		return {}

	return search_results

def getdbConnectionToCollection(db, collection):
	db_conn = DbManager(Config.get("MONGO_DB_URI"), Config.get(db), Config.get(collection))
	return db_conn

def validateUnlockChapterQuery(db_conn_user, query, series_id):
	try:
		record = db_conn_user.findOne(query)
		print(record)

		for series in record["series-unlocked"]:
			if series["series-id"] == series_id:
				if series["unlocked-chapters"] < series["total-chapters"]:
					return True
	except Exception as e:
		logger.error(e)

	return False

def unlockChapterForUser(user_id, series_id):
	db_conn_user = getdbConnectionToCollection("MONGO_USER_DB", "MONGO_USER_COLLECTION")
	query = {"user-id": user_id, "series-unlocked.series-id": series_id}

	if not validateUnlockChapterQuery(db_conn_user, query, series_id):
		logger.error("Could not unlock Chpater")
		return False

	update_query = {"$inc": {"series-unlocked.$.unlocked-chapters": 1}}

	try:
		db_conn_user.update(query, update_query)
	except Exception as e:
		logger.error("Unable to unlock new chapter")
		return False
		
	return True

def addUserInfo(user_detail_response, users_info):
	user_detail_response["name"] = users_info["name"]
	user_detail_response["email"] = users_info["email"]

	return user_detail_response

def addSeriesDetail(series, series_data, series_detail):
	series_detail["series-name"] = series_data["series"]["name"]
	series_detail["total-chapters"] = series_data["series"]["total-chapters"]
	series_detail["unlocked-chapters"] = series["unlocked-chapters"]

	return series_detail

def addSeriesAndChaptersDetail(series):
	series_detail = {}
	db_conn_content = getdbConnectionToCollection("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")
	
	series_data = db_conn_content.findOne({"series.series-id": series["series-id"]})
	
	series_detail = addSeriesDetail(series, series_data, series_detail)
	
	series_detail["unlocked-chapters-metadata"] = []
	
	for i in range(series["unlocked-chapters"]):
		series_detail["unlocked-chapters-metadata"].append(series_data["chapters"][i]["content-metadata"])

	return series_detail

def extractUserDetails(userId):
	db_conn_user = getdbConnectionToCollection("MONGO_USER_DB", "MONGO_USER_COLLECTION")

	try:
		users_info = db_conn_user.findOne({"user-id": userId})

		user_detail_response = {}
		user_detail_response = addUserInfo(user_detail_response, users_info)
		
		user_detail_response["unlocked-series"] = []
		
		for series in users_info["series-unlocked"]:
			series_detail = addSeriesAndChaptersDetail(series)
			user_detail_response["unlocked-series"].append(series_detail)
	except Exception as e:
		logger.error(e)
		return {}

	# print(user_detail_response)

	return user_detail_response


def extractContentDetails():
	db_conn_content = getdbConnectionToCollection("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")

	try:
		series_data = db_conn_content.findAll({})
	except Exception as e:
		logger.error(e)
		return {}

	return series_data




