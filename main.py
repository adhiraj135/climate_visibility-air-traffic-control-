from flask import Flask,render_template,request,Response
from flask_cors import CORS,cross_origin
from training_data_validation import data_validatation_and_transformation
from prediction_data_validation import data_validatation_and_transformation
from model_training import training_model
from model_prediction import prediction_model
import os
import flask_monitoringdashboard as dashboard

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path = request.json['filepath']

            pred_val = data_validatation_and_transformation(path)

            pred_val.validate_and_transform()

            pred = prediction_model(path)

            path = pred.predict()
            return Response("Prediction File created at %s!!!" % path)
        elif request.form is not None:
            path = request.form['filepath']

            pred_val = data_validatation_and_transformation(path)

            pred_val.validate_and_transform()

            pred = prediction_model(path)

            path = pred.predict()
            return Response("Prediction File created at %s!!!" % path)

    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)



@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            train_valObj = data_validatation_and_transformation(path)

            train_valObj.validate_and_transform()


            trainModelObj = training_model()
            trainModelObj.training_model()

    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")


if __name__ == "__main__":
    app.run(host="0.0.0.0",port="8080")


