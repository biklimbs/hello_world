from flask import Flask,render_template,request,make_response,jsonify
import random
from total_car_count import *
import flask
from recommedation.recommendation_sys import *
from recommedation.constants_vikas import *

from recommedation import *
app = Flask(__name__)

#---Global variable---
make_g=""
model_g=""
year_g=""
low_mileage_g=0
high_mileage_g=0
rating_g=[]
kuzov_g=""
mileage_g=""

# print(make_name)

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
    return {'fulfillmentText': str(rand_num)}

#---Returns mileage range based on the selected option---
def select_mileage(input_option):
    switcher = { 
        1: "0 and 20000", 
        2: "20001 and 40000", 
        3: "40001 and 60000",
        4: "60001 and 80000", 
        5: "80001 and 100000", 
        6: "100001 and 120000", 
        7: "120001 and 140000",
        8: "140001 and 160000", 
        9: ">160000" 
    }
    try:
        return switcher[input_option],True
    except Exception as e:
        return str(e),False

#---Maps given number to corresonding rate---
def map_rating(input_option):
    switcher = { 
        'a': "s", 
        'b': "6",
        'c': "5", 
        'd': "4.5", 
        'e': "4",
        'f': "3.5",
        'g': "3",
        'h': "2",
        'i': "1",
        'j': "RA",
        'k': "R" 
    }
    try:
        return switcher[input_option]
    except Exception as e:
        return str(e)

# create a route for webhook
@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    global make_g,model_g,year_g,rating_g,low_mileage_g,high_mileage_g,kuzov_g,mileage_g
    # make_name= "vikas"
    req = request.get_json(silent=True)
    #print(make_name)
    print(req)
    intend_name=req.get('queryResult').get('intent').get('displayName')
    if str(intend_name)=='country':
        total_vehicle_count=get_total_count()
        welcome_text="Today we have "+str(total_vehicle_count)+" vehicle in auction. Do you want us to show what we have?"
        print(welcome_text)
        res_str={'fulfillmentText': str(welcome_text)}

    elif str(intend_name)=='first_customer_interaction':
        make_list=str(get_all_make())+"\nAvailable Makes for auction are shown above: "
        #make_list=get_all_make()
        res_str={'fulfillmentText': str(make_list)}

    elif str(intend_name)=='make':
        make_name=req.get('queryResult').get('parameters').get('make')[0]
        print(make_name)
        make_g=make_name
        model_list=str(get_all_model(make_name))+"\nAvailable Model for "+make_g+" are shown above: "
        res_str={'fulfillmentText': str(model_list)}
    
    elif str(intend_name)=='model': 
        model_name=req.get('queryResult').get('parameters').get('model')[0]
        model_g=model_name
        kuzov_list=str(get_all_kuzov(make_g,model_name))+"\nAvailable Kuzov for "+make_g+" and "+model_g+" are shown above: "
        res_str={'fulfillmentText': str(kuzov_list)}
    
    elif str(intend_name)=='kuzov': 
        kuzov_g=req.get('queryResult').get('parameters').get('kuzov')[0]
        print(make_g,model_g,year_g)
        year_list=str(get_all_years(make_g,model_g,kuzov_g))+"\nAvailable Year for auction are shown above:"
        #year_list=get_all_mileage(make_g,model_g,year_g)
        res_str={'fulfillmentText': str(year_list)}
    
    elif str(intend_name)=='year':
        year_g=req.get('queryResult').get('parameters').get('year')[0]
        print(make_g,model_g,year_g,kuzov_g)
        mileage_list="\n1. Less than 20000 \n2. 20001-40000\n3. 40001-60000 \n4. 60001-80000\
        \n5. 80001-100000\n6. 100001-120000 \n7. 120001-140000\n8. 140001-160000 \n9. Above 160000\
        \nPlease select mileage from above" 
        #mileage_list=get_all_mileage(make_g,model_g,year_g,kuzov_g)
        res_str={'fulfillmentText': str(mileage_list)}
    
    elif str(intend_name)=='mileage': 
        mileage_option=req.get('queryResult').get('parameters').get('mileage')[0]
        mileage_option=int(mileage_option)
        mileage_g,mileage_status=select_mileage(mileage_option)
        if mileage_status:
            if make_g!='' and model_g!='':
                print(make_g,model_g,mileage_g,year_g,kuzov_g)
                rating_list,rating_status=get_all_rate(make_g,model_g,mileage_g,year_g,kuzov_g)
                if rating_status:
                    rating_select_option="\na. <10,000km 12 months \nb. <30,000km 36 months\nc. <50,000km. \nd. <100,000km\
                    \ne. <150000km\nf. Noticeable large scratches \ng .Many exterior scratches \nh.Bad condition\ni. Flood damaged \nj. Minor Damaged and repair \nk. Damaged and repaired \n Available conditions of cars are given above.\nPlease select from given above: "
                    low_mileage_g,high_mileage_g=mileage_g.replace("and","-").split("-")
                    mileage_rc=int((int(str(low_mileage_g).strip())+int(str(high_mileage_g).strip()))/2)
                    rating=rating_select_option
                    #rating="Please select rating from given list: "+str(rating_list)
                else:
                    rating="No data was found for given specification"
                    mileage_g=""
            else:
                rating="please supply valid make and models"
        else:
            rating="please select valid mileage options"
            mileage_g=""
        res_str={'fulfillmentText': str(rating)}
    
    elif str(intend_name)=='rating': 
        #print(make_name)
        rating_g=req.get('queryResult').get('parameters').get('rating')
        print(rating_g)
        #print(map_rating(rating_g))
        rate_rc=map_rating(rating_g[0])
        rating_g=list(map(map_rating,rating_g))
        print(rating_g)
        rating_g = json.dumps(rating_g)
        #rating_str="you have selected following rating: "+str(rating_g)+" Proceed to get price of the car:"

        if make_g!='' and model_g!='' and str(year_g)!='' and str(kuzov_g)!='':
            #response_dict=call_bid_lambda("ARU","6","1",make_g,model_g,year_g,"2018","",kuzov_g,[],[],rating_g,[])
            response_dict=call_bid_lambda("ARU","6","1",make_g,model_g,year_g,"2018",str(low_mileage_g).strip()+"-"+str(high_mileage_g).strip(),kuzov_g,[],[],rating_g,[])
            print(response_dict)
            try:
                d = json.loads(response_dict)
            except Exception as e:
                print(str(e))
            price="The price for "+d["company"]+" "+d["model"]+" having Kuzov "+d["kuzov"]+" is "+str(d["price"])+"\n Do you want recommendations for specified car: "
            res_str={'fulfillmentText': str(price)}
        else:
            mileage_list="please supply valid make and models"
            res_str={'fulfillmentText': str(mileage_list)}
        #res_str={'fulfillmentText': str(rating_str)}


    elif str(intend_name)=='recommendation_action':
        if make_g!='' and model_g!='' and str(year_g)!='' and str(kuzov_g)!='':
            car_details_str,recomm_status=get_recommend_cars(make_g, model_g, year_g,"20000", kuzov_g, "", "", "3.5", "")
            if recomm_status:
                res_str={'fulfillmentText': str(car_details_str)}
            else:
                res_str={'fulfillmentText': str("No recommendation available")}
    else:
        res_str={'fulfillmentText': str("Sorry intend could not be identified")}
    return make_response(jsonify(res_str))
    

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


def get_make_model_count(make,model):
    try:
        df=get_make_model_total_count(make,model)
        return "Total car for "+make+" "+model+": "+str(df.iloc[0,0])
    except Exception as e:
        print(str(e))

#---function starts here---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=89)