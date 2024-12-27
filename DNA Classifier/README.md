# Gene-Sequence-Classification

### Overview
The aim of the project is to preprocess promoter gene sequence data and evaluate the performance of multiple machine learning models in classifying the sequences. It achieves this by converting sequences into numerical data, applying various classification algorithms, performing cross-validation, and comparing the models' performance based on accuracy and classification metrics. This approach ensures a robust and comprehensive evaluation of different machine learning techniques on the dataset.

### Data Source
The dataset used for this analysis was sourced from the Machine Learning Repository at the UCI

### Tools
-Google Colab. [Download here](https://colab.research.google.com/)

### Project Summary

- **Library Import**: The necessary library for data handlin, preprocessing, model building and evaluation
- **Data Loading** - The dataset was loaded directly from UCI repository
- **Data Preprocessing** - The data sequence was split into individual nucleotide and encoded as numerical values using **LabelEncoer**
- **Data Splitting** - The features and labels were separated an the data was split into training and testing sets.
- **Model Definition**- defined a set of machine learning models including Random Forest, AdaBoost, KNN, Naive Bayes, and different SVMKernels.
- **Cross-Validation:** Perform 10-fold cross-validation to evaluate each model, printing their mean accuracy and standard deviation.
- **Model Training and Evaluation:** Trained each model on the training set and evaluate them on the test set, printing the accuracy and classification report for each.

### Result
The result showed that the adaBoost gave the highest accuracy



