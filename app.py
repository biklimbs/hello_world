from flask import Flask,render_template,request,make_response,jsonify
import random
from total_car_count import *
import flask
app = Flask(__name__)

#@app.route('/random_num')
#def test():
'''
@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    #rand_num=str(random.randint(0, 10))
    #print(rand_num)
    #rand_num_dict={}
    #rand_num_dict["rand_num"]=rand_num
    req = request.get_json(force=True)
    action = req.get('queryResult').get('action')
    print(action)
    #{"payload": {"google": {"expectUserResponse": True,"richResponse": {"items": [{"simpleResponse": {"textToSpeech": "this is a simple response"}}]}}}}
    return flask.make_response(flask.jsonify({"responseId": "ea3d77e8-ae27-41a4-9e1d-174bd461b68c","session": "projects/your-agents-project-id/agent/sessions/88d13aa8-2999-4f71-b233-39cbf3a824a0","queryResult": {"queryText": "user's original query to your agent","parameters": {"param": "param value"},"allRequiredParamsPresent": True,"fulfillmentText": "Text defined in Dialogflow's console for the intent that was matched","fulfillmentMessages": [{"text": {"text": ["Text defined in Dialogflow's console for the intent that was matched"]}}],"outputContexts": [{"name": "projects/your-agents-project-id/agent/sessions/88d13aa8-2999-4f71-b233-39cbf3a824a0/contexts/generic","lifespanCount": 5,"parameters": {"param": "param value"}}],"intent": {"name": "projects/your-agents-project-id/agent/intents/29bcd7f8-f717-4261-a8fd-2d3e451b8af8","displayName": "Matched Intent Name"},"intentDetectionConfidence": 1,"diagnosticInfo": {},"languageCode": "en"},"originalDetectIntentRequest": {}}))

    #return flask.jsonify(rand_num_dict)
'''
# function for responses
def results():
    # build a request object
    req = request.get_json(force=True)

    # fetch action from json
    action = req.get('queryResult').get('number')
    print("request from chatbot: "+str(req))
    print("entity :"+str(action))
    # return a fulfillment response
    rand_num=str(random.randint(0, 10))
    print(rand_num)
    #rand_num_dict={}
    #rand_num_dict["rand_num"]=rand_num
    return {'fulfillmentText': str(rand_num)}


def total_car_count():
    # build a request object
    req = request.get_json(force=True)

    # fetch action from json
    action = req.get('queryResult').get('number')
    print("request from chatbot: "+str(req))
    print("entity :"+str(action))
    # return a fulfillment response
    rand_num=str(random.randint(50000,75000))
    print(rand_num)
    #rand_num_dict={}
    #rand_num_dict["rand_num"]=rand_num
    return {'fulfillmentText': str(rand_num)}



# create a route for webhook
@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    # return response
    #res_str=results()
    if request.headers.get('Content-Type') == 'application/json':
        req = request.get_json(silent=True)

        # fetch action from json
        #action = req.get('queryResult').get('number')
        if req.get("result").get("action") == "@total":
            rand_num=str(random.randint(50000,75000))
            res_str={'fulfillmentText': str(rand_num)}
        else:
            print("different action")
        #print("Response to chatbot: "+str(res_str))
        return make_response(jsonify(res_str))
    else:
        print("request not in proper format")


# create a route for webhook
@app.route('/total_car_in_auction', methods=['GET', 'POST'])
def total_car_in_auction():
    # return response
    
    res_str=total_car_count()
    print("Response to chatbot: "+str(res_str))
    return make_response(jsonify(res_str))

@app.route('/get_total_count')
def get_total_car_count():
    df=get_total_count()
    return "Total car: "+str(df.iloc[0,0])

@app.route('/get_make_model_count')
def get_make_model_count():
    try:
        if request.method == 'POST':
            make = request.form['make']
            model = request.form['model']
            df=get_make_model_total_count(make,model)
            return "Total car for "+make+" "+model+": "+str(df.iloc[0,0])
    except Exception as e:
        print(str(e))


'''
# create a route for webhook
@app.route('/model_count', methods=['GET', 'POST'])
def total_car_in_auction():
    # return response
    
    res_str=total_car_count()
    print("Response to chatbot: "+str(res_str))
    return make_response(jsonify(res_str))
'''
#---function starts here---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=89)