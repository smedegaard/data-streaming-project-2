from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        print(f'{kwargs}')
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data ✅
    def generate_data(self):
        with open(self.input_file, 'r') as file:
            data = json.load(file)
            for event in data:
                message = self.dict_to_binary(event)
                # TODO send the correct data ✅
                self.send(self.topic, message)
                time.sleep(0.1)
                 

    # TODO fill this in to return the json dictionary to binary ✅
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
        