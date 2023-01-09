from training_data_validation import data_validatation_and_transformation
from model_training import training_model




def train(path):
    train_val=data_validatation_and_transformation(path)
    train_val.validate_and_transform()

    train_model=training_model()
    train_model.training_model()


if __name__=="__main__":
    train('Training_Batch_Files/')