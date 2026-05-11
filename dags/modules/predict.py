import os
import json
import glob
import pandas as pd
import dill
from datetime import datetime


path = os.path.dirname(os.path.abspath(__file__))
path = os.path.dirname(path)

def predict():
    models_dir = os.path.join(path, 'data', 'models')
    test_dir = os.path.join(path, 'data', 'test')
    output_dir = os.path.join(path, 'data', 'predictions')
    os.makedirs(output_dir, exist_ok=True)

    pkl_files = glob.glob(os.path.join(models_dir, '*.pkl'))
    if not pkl_files:
        raise FileNotFoundError(f"No model file found in {models_dir}")
    model_path = pkl_files[0]
    with open(model_path, 'rb') as f:
        model = dill.load(f)
    print(f"Loaded model from {model_path}")

    test_files = glob.glob(os.path.join(test_dir, '*.json'))
    if not test_files:
        raise FileNotFoundError(f"No test files found in {test_dir}")

    predictions_list = []
    for file in test_files:
        with open(file, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame.from_dict([data])
        pred = model.predict(df)[0]
        record_id = data.get('id', os.path.basename(file).split('.')[0])
        predictions_list.append({'id': record_id, 'prediction': pred})
        print(f"Processed {file}, prediction: {pred}")

    result = pd.DataFrame(predictions_list)
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    output_path = os.path.join(output_dir, f'preds_{timestamp}.csv')
    result.to_csv(output_path, index=False)
    print(f"Predictions saved to {output_path}")

if __name__ == '__main__':
    predict()