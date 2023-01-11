from flask import Flask,render_template,request,Response
from flask_cors import CORS,cross_origin
from training_data_validation import training_data_validatation_and_transformation
from prediction_data_validation import prediction_data_validatation_and_transformation
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
def predict():
    try:
        if request.form is not None:
           path = request.form
           Prediction_path=path[list(path.keys())[0]]

           pred_val = prediction_data_validatation_and_transformation(Prediction_path)

           pred_val.prediction_validate_and_transform()

           pred = prediction_model(Prediction_path)

           result = pred.predict()

        elif request.json is not None:
            Prediction_path = request.json['filepath']

            pred_val = prediction_data_validatation_and_transformation(Prediction_path)

            pred_val.prediction_validate_and_transform()

            pred = prediction_model(Prediction_path)

            result = pred.predict()
        return render_template('index.html', result_text="The path of the prediction file is {}".format(result))

    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)



@app.route("/train", methods=['POST'])
@cross_origin()
def train():

    try:
        if request.form is not None:
            path = request.form
            training_path=path[list(path.keys())[0]]

            train_val = training_data_validatation_and_transformation(training_path)
            train_val.training_validate_and_transform()

            train=training_model()
            train.training_model()

        if request.json is not None:
            training_path = request.json['folderpath']

            training_val = training_data_validatation_and_transformation(training_path)

            training_val.training_validate_and_transform()

            train = training_model()

            train.training_model()



        return render_template('index.html', result="Training completion is a success")


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)


if __name__ == "__main__":
    app.run(host="0.0.0.0",port="6060")


