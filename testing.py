from prediction_data_validation import data_validatation_and_transformation
from model_prediction import prediction_model

def predict(path):
    pred_val=data_validatation_and_transformation(path)
    pred_val.validate_and_transform()


    pred=prediction_model(path)
    pred.predict()


if __name__=="__main__":
    predict('Prediction_Batch_files/')

