class Config:
	__conf = {
	"KAFKA_SERVERS": ["127.0.0.1:9092"],
	"REQUEST_MANAGER_TOPIC":"upload_request",
	"KAFKA_CONSUMER_GROUP": "default-test-consumer",
	"BULK_CONTENT_UPLOAD_TOPIC":"upload_new_content",
	"BULK_USER_UPLOAD_TOPIC":"upload_new_user",
	"MONGO_DB_URI": "127.0.0.1:27017",
	"MONGO_CONTENT_DB":"ptlp_content_db",
	"MONGO_USER_DB": "ptlp_user_db",
	"MONGO_CONTENT_COLLECTION":"content",
	"MONGO_USER_COLLECTION":"users",
	}

	@staticmethod
	def get(prop):
		return Config.__conf[prop]

	@staticmethod
	def set(prop, value):
		Config.__conf[name] = value