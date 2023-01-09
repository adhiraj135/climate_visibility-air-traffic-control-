from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import r2_score


class model_finder():

    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object
        self.xgb = XGBRegressor()
        self.dt = DecisionTreeRegressor()

    def get_best_param_for_dt(self, x_train, y_train):
        self.logger_object.log(self.file_object, 'Entered the best param for dt method of model finder class')
        try:
            dt_param = {
            "criterion": ['squared_error', 'friedman_mse', 'absolute_error', 'poisson'],
            "splitter": ['best', 'random'],
            "max_depth": range(2, 16, 2),
            "max_features": ["auto", "sqrt", "log2"],
            "min_samples_split": range(2, 16, 2)
            }

            grid_dt = GridSearchCV(estimator=DecisionTreeRegressor(), param_grid=dt_param, verbose=3, cv=5)
            grid_dt.fit(x_train, y_train)

            criterion = grid_dt.best_params_['criterion']
            splitter = grid_dt.best_params_['splitter']
            max_depth = grid_dt.best_params_['max_depth']
            max_features = grid_dt.best_params_['max_features']
            min_samples_split = grid_dt.best_params_['min_samples_split']

            self.dt = DecisionTreeRegressor(criterion=criterion, splitter=splitter, max_depth=max_depth,
                                        max_features=max_features, min_samples_split=min_samples_split)
            self.dt.fit(x_train, y_train)
            self.logger_object.log(self.file_object, "best params are" +"\t"+ "criterion: %s"%criterion +"\t"+ "splitter : %s"%splitter +"\t"+ "max_depth: %s"%max_depth +"\t"+ "max_features:%s"%max_features +"\t"+ "min_samples_split:%s"%min_samples_split +"\t"+ "Exited the best param for dt method of model finder class"+"\n")
            return self.dt

        except Exception as e:
            self.logger_object.log(self.file_object,"exception occured in best param for dt method : %s"%e)
            self.logger_object.log(self.file_object,"best param for dt method unsuccessful,Exited the best param for dt method of model finder class")
            raise e




    def get_param_for_xgboost(self, x_train, y_train):
        self.logger_object.log(self.file_object, 'Entered the best param for xgboost method of model finder class')
        try:
           xgb_param = {
            "learning_rate": [0.01, .1, 0.5, .001],
            "max_depth": [3, 5, 10, 16],
            "n_estimators": [10, 50, 100, 200]
           }

           grid_xgb = GridSearchCV(estimator=XGBRegressor(objective='reg:linear'), param_grid=xgb_param, verbose=3, cv=5)
           grid_xgb.fit(x_train, y_train)

           learning_rate = grid_xgb.best_params_['learning_rate']
           max_depth = grid_xgb.best_params_['max_depth']
           n_estimators = grid_xgb.best_params_['n_estimators']

           self.xgb = XGBRegressor(objective='reg:linear', learning_rate=learning_rate, max_depth=max_depth,
                                n_estimators=n_estimators)
           self.xgb.fit(x_train, y_train)
           self.logger_object.log(self.file_object, "best param for xgboost are" +"\t"+ "learning_rate : %s"%learning_rate +"\t"+"max_depth : %s"%max_depth+"\t"+"n_estimators : %s"%n_estimators+"\t"+"Entered the best param for dt method of model finder class"+"\n")

           return self.xgb

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in xgboost method : %s'%e)
            self.logger_object.log(self.file_object, 'best param for xgboost method unsuccessful.Exited the best param for xgboost method of model finder class')
            raise e


    def best_model(self, x_train, y_train, x_test, y_test):
        self.logger_object.log(self.file_object, 'Entered the best model method of model finder class')
        try:
            decision_tree_reg = self.get_best_param_for_dt(x_train, y_train)
            predicted_y = decision_tree_reg.predict(x_test)
            dt_score = r2_score(y_test, predicted_y)

            xgboost_reg = self.get_param_for_xgboost(x_train, y_train)
            predict_y = xgboost_reg.predict(x_test)
            xgb_score = r2_score(y_test, predict_y)

            if dt_score < xgb_score:
                self.logger_object.log(self.file_object,'best model is : %s,Exited the best model method of model finder class'%'xgboost')
                return 'xgboost', xgboost_reg
            else:
                self.logger_object.log(self.file_object, 'best model is : %s,Exited the best model method of model finder class'%'decision_tree')
                return 'decisiontree', decision_tree_reg

        except Exception as e:
            self.logger_object.log(self.file_object, 'exception in best model method %s'%e)
            self.logger_object.log(self.file_object, 'best model method unscucessful,Exited the best model method of model finder class')
            raise e

