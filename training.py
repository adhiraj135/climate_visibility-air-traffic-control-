from training_data_validation import training_data_validatation_and_transformation
from model_training import training_model




def train(path):
    train_val=training_data_validatation_and_transformation(path)
    train_val.training_validate_and_transform()



if __name__=="__main__":
    train('Training_Batch_Files/')