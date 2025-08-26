defmodule Google.Cloud.Bigquery.V2.RemoteModelInfo.RemoteServiceType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :REMOTE_SERVICE_TYPE_UNSPECIFIED, 0
  field :CLOUD_AI_TRANSLATE_V3, 1
  field :CLOUD_AI_VISION_V1, 2
  field :CLOUD_AI_NATURAL_LANGUAGE_V1, 3
  field :CLOUD_AI_SPEECH_TO_TEXT_V2, 7
end

defmodule Google.Cloud.Bigquery.V2.Model.ModelType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :MODEL_TYPE_UNSPECIFIED, 0
  field :LINEAR_REGRESSION, 1
  field :LOGISTIC_REGRESSION, 2
  field :KMEANS, 3
  field :MATRIX_FACTORIZATION, 4
  field :DNN_CLASSIFIER, 5
  field :TENSORFLOW, 6
  field :DNN_REGRESSOR, 7
  field :XGBOOST, 8
  field :BOOSTED_TREE_REGRESSOR, 9
  field :BOOSTED_TREE_CLASSIFIER, 10
  field :ARIMA, 11
  field :AUTOML_REGRESSOR, 12
  field :AUTOML_CLASSIFIER, 13
  field :PCA, 14
  field :DNN_LINEAR_COMBINED_CLASSIFIER, 16
  field :DNN_LINEAR_COMBINED_REGRESSOR, 17
  field :AUTOENCODER, 18
  field :ARIMA_PLUS, 19
  field :ARIMA_PLUS_XREG, 23
  field :RANDOM_FOREST_REGRESSOR, 24
  field :RANDOM_FOREST_CLASSIFIER, 25
  field :TENSORFLOW_LITE, 26
  field :ONNX, 28
  field :TRANSFORM_ONLY, 29
  field :CONTRIBUTION_ANALYSIS, 37
end

defmodule Google.Cloud.Bigquery.V2.Model.LossType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :LOSS_TYPE_UNSPECIFIED, 0
  field :MEAN_SQUARED_LOSS, 1
  field :MEAN_LOG_LOSS, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.DistanceType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DISTANCE_TYPE_UNSPECIFIED, 0
  field :EUCLIDEAN, 1
  field :COSINE, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.DataSplitMethod do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATA_SPLIT_METHOD_UNSPECIFIED, 0
  field :RANDOM, 1
  field :CUSTOM, 2
  field :SEQUENTIAL, 3
  field :NO_SPLIT, 4
  field :AUTO_SPLIT, 5
end

defmodule Google.Cloud.Bigquery.V2.Model.DataFrequency do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DATA_FREQUENCY_UNSPECIFIED, 0
  field :AUTO_FREQUENCY, 1
  field :YEARLY, 2
  field :QUARTERLY, 3
  field :MONTHLY, 4
  field :WEEKLY, 5
  field :DAILY, 6
  field :HOURLY, 7
  field :PER_MINUTE, 8
end

defmodule Google.Cloud.Bigquery.V2.Model.HolidayRegion do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :HOLIDAY_REGION_UNSPECIFIED, 0
  field :GLOBAL, 1
  field :NA, 2
  field :JAPAC, 3
  field :EMEA, 4
  field :LAC, 5
  field :AE, 6
  field :AR, 7
  field :AT, 8
  field :AU, 9
  field :BE, 10
  field :BR, 11
  field :CA, 12
  field :CH, 13
  field :CL, 14
  field :CN, 15
  field :CO, 16
  field :CS, 17
  field :CZ, 18
  field :DE, 19
  field :DK, 20
  field :DZ, 21
  field :EC, 22
  field :EE, 23
  field :EG, 24
  field :ES, 25
  field :FI, 26
  field :FR, 27
  field :GB, 28
  field :GR, 29
  field :HK, 30
  field :HU, 31
  field :ID, 32
  field :IE, 33
  field :IL, 34
  field :IN, 35
  field :IR, 36
  field :IT, 37
  field :JP, 38
  field :KR, 39
  field :LV, 40
  field :MA, 41
  field :MX, 42
  field :MY, 43
  field :NG, 44
  field :NL, 45
  field :NO, 46
  field :NZ, 47
  field :PE, 48
  field :PH, 49
  field :PK, 50
  field :PL, 51
  field :PT, 52
  field :RO, 53
  field :RS, 54
  field :RU, 55
  field :SA, 56
  field :SE, 57
  field :SG, 58
  field :SI, 59
  field :SK, 60
  field :TH, 61
  field :TR, 62
  field :TW, 63
  field :UA, 64
  field :US, 65
  field :VE, 66
  field :VN, 67
  field :ZA, 68
end

defmodule Google.Cloud.Bigquery.V2.Model.ColorSpace do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :COLOR_SPACE_UNSPECIFIED, 0
  field :RGB, 1
  field :HSV, 2
  field :YIQ, 3
  field :YUV, 4
  field :GRAYSCALE, 5
end

defmodule Google.Cloud.Bigquery.V2.Model.LearnRateStrategy do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :LEARN_RATE_STRATEGY_UNSPECIFIED, 0
  field :LINE_SEARCH, 1
  field :CONSTANT, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.OptimizationStrategy do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :OPTIMIZATION_STRATEGY_UNSPECIFIED, 0
  field :BATCH_GRADIENT_DESCENT, 1
  field :NORMAL_EQUATION, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.FeedbackType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :FEEDBACK_TYPE_UNSPECIFIED, 0
  field :IMPLICIT, 1
  field :EXPLICIT, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.SeasonalPeriod.SeasonalPeriodType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :SEASONAL_PERIOD_TYPE_UNSPECIFIED, 0
  field :NO_SEASONALITY, 1
  field :DAILY, 2
  field :WEEKLY, 3
  field :MONTHLY, 4
  field :QUARTERLY, 5
  field :YEARLY, 6
end

defmodule Google.Cloud.Bigquery.V2.Model.KmeansEnums.KmeansInitializationMethod do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :KMEANS_INITIALIZATION_METHOD_UNSPECIFIED, 0
  field :RANDOM, 1
  field :CUSTOM, 2
  field :KMEANS_PLUS_PLUS, 3
end

defmodule Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.BoosterType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :BOOSTER_TYPE_UNSPECIFIED, 0
  field :GBTREE, 1
  field :DART, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.DartNormalizeType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :DART_NORMALIZE_TYPE_UNSPECIFIED, 0
  field :TREE, 1
  field :FOREST, 2
end

defmodule Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.TreeMethod do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TREE_METHOD_UNSPECIFIED, 0
  field :AUTO, 1
  field :EXACT, 2
  field :APPROX, 3
  field :HIST, 4
end

defmodule Google.Cloud.Bigquery.V2.Model.HparamTuningEnums.HparamTuningObjective do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :HPARAM_TUNING_OBJECTIVE_UNSPECIFIED, 0
  field :MEAN_ABSOLUTE_ERROR, 1
  field :MEAN_SQUARED_ERROR, 2
  field :MEAN_SQUARED_LOG_ERROR, 3
  field :MEDIAN_ABSOLUTE_ERROR, 4
  field :R_SQUARED, 5
  field :EXPLAINED_VARIANCE, 6
  field :PRECISION, 7
  field :RECALL, 8
  field :ACCURACY, 9
  field :F1_SCORE, 10
  field :LOG_LOSS, 11
  field :ROC_AUC, 12
  field :DAVIES_BOULDIN_INDEX, 13
  field :MEAN_AVERAGE_PRECISION, 14
  field :NORMALIZED_DISCOUNTED_CUMULATIVE_GAIN, 15
  field :AVERAGE_RANK, 16
end

defmodule Google.Cloud.Bigquery.V2.Model.CategoryEncodingMethod.EncodingMethod do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :ENCODING_METHOD_UNSPECIFIED, 0
  field :ONE_HOT_ENCODING, 1
  field :LABEL_ENCODING, 2
  field :DUMMY_ENCODING, 3
end

defmodule Google.Cloud.Bigquery.V2.Model.PcaSolverOptionEnums.PcaSolver do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :UNSPECIFIED, 0
  field :FULL, 1
  field :RANDOMIZED, 2
  field :AUTO, 3
end

defmodule Google.Cloud.Bigquery.V2.Model.ModelRegistryOptionEnums.ModelRegistry do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :MODEL_REGISTRY_UNSPECIFIED, 0
  field :VERTEX_AI, 1
end

defmodule Google.Cloud.Bigquery.V2.Model.HparamTuningTrial.TrialStatus do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TRIAL_STATUS_UNSPECIFIED, 0
  field :NOT_STARTED, 1
  field :RUNNING, 2
  field :SUCCEEDED, 3
  field :FAILED, 4
  field :INFEASIBLE, 5
  field :STOPPED_EARLY, 6
end

defmodule Google.Cloud.Bigquery.V2.RemoteModelInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:remote_service, 0)

  field :endpoint, 1, type: :string, oneof: 0, deprecated: false

  field :remote_service_type, 2,
    type: Google.Cloud.Bigquery.V2.RemoteModelInfo.RemoteServiceType,
    json_name: "remoteServiceType",
    enum: true,
    oneof: 0,
    deprecated: false

  field :connection, 3, type: :string, deprecated: false
  field :max_batching_rows, 4, type: :int64, json_name: "maxBatchingRows", deprecated: false

  field :remote_model_version, 5,
    type: :string,
    json_name: "remoteModelVersion",
    deprecated: false

  field :speech_recognizer, 7, type: :string, json_name: "speechRecognizer", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.TransformColumn do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string, deprecated: false
  field :type, 2, type: Google.Cloud.Bigquery.V2.StandardSqlDataType, deprecated: false
  field :transform_sql, 3, type: :string, json_name: "transformSql", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.Model.SeasonalPeriod do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.KmeansEnums do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.HparamTuningEnums do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.RegressionMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :mean_absolute_error, 1, type: Google.Protobuf.DoubleValue, json_name: "meanAbsoluteError"
  field :mean_squared_error, 2, type: Google.Protobuf.DoubleValue, json_name: "meanSquaredError"

  field :mean_squared_log_error, 3,
    type: Google.Protobuf.DoubleValue,
    json_name: "meanSquaredLogError"

  field :median_absolute_error, 4,
    type: Google.Protobuf.DoubleValue,
    json_name: "medianAbsoluteError"

  field :r_squared, 5, type: Google.Protobuf.DoubleValue, json_name: "rSquared"
end

defmodule Google.Cloud.Bigquery.V2.Model.AggregateClassificationMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :precision, 1, type: Google.Protobuf.DoubleValue
  field :recall, 2, type: Google.Protobuf.DoubleValue
  field :accuracy, 3, type: Google.Protobuf.DoubleValue
  field :threshold, 4, type: Google.Protobuf.DoubleValue
  field :f1_score, 5, type: Google.Protobuf.DoubleValue, json_name: "f1Score"
  field :log_loss, 6, type: Google.Protobuf.DoubleValue, json_name: "logLoss"
  field :roc_auc, 7, type: Google.Protobuf.DoubleValue, json_name: "rocAuc"
end

defmodule Google.Cloud.Bigquery.V2.Model.BinaryClassificationMetrics.BinaryConfusionMatrix do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :positive_class_threshold, 1,
    type: Google.Protobuf.DoubleValue,
    json_name: "positiveClassThreshold"

  field :true_positives, 2, type: Google.Protobuf.Int64Value, json_name: "truePositives"
  field :false_positives, 3, type: Google.Protobuf.Int64Value, json_name: "falsePositives"
  field :true_negatives, 4, type: Google.Protobuf.Int64Value, json_name: "trueNegatives"
  field :false_negatives, 5, type: Google.Protobuf.Int64Value, json_name: "falseNegatives"
  field :precision, 6, type: Google.Protobuf.DoubleValue
  field :recall, 7, type: Google.Protobuf.DoubleValue
  field :f1_score, 8, type: Google.Protobuf.DoubleValue, json_name: "f1Score"
  field :accuracy, 9, type: Google.Protobuf.DoubleValue
end

defmodule Google.Cloud.Bigquery.V2.Model.BinaryClassificationMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :aggregate_classification_metrics, 1,
    type: Google.Cloud.Bigquery.V2.Model.AggregateClassificationMetrics,
    json_name: "aggregateClassificationMetrics"

  field :binary_confusion_matrix_list, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.BinaryClassificationMetrics.BinaryConfusionMatrix,
    json_name: "binaryConfusionMatrixList"

  field :positive_label, 3, type: :string, json_name: "positiveLabel"
  field :negative_label, 4, type: :string, json_name: "negativeLabel"
end

defmodule Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix.Entry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :predicted_label, 1, type: :string, json_name: "predictedLabel"
  field :item_count, 2, type: Google.Protobuf.Int64Value, json_name: "itemCount"
end

defmodule Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix.Row do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :actual_label, 1, type: :string, json_name: "actualLabel"

  field :entries, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix.Entry
end

defmodule Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :confidence_threshold, 1,
    type: Google.Protobuf.DoubleValue,
    json_name: "confidenceThreshold"

  field :rows, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix.Row
end

defmodule Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :aggregate_classification_metrics, 1,
    type: Google.Cloud.Bigquery.V2.Model.AggregateClassificationMetrics,
    json_name: "aggregateClassificationMetrics"

  field :confusion_matrix_list, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics.ConfusionMatrix,
    json_name: "confusionMatrixList"
end

defmodule Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue.CategoricalValue.CategoryCount do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :category, 1, type: :string
  field :count, 2, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue.CategoricalValue do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :category_counts, 1,
    repeated: true,
    type:
      Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue.CategoricalValue.CategoryCount,
    json_name: "categoryCounts"
end

defmodule Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:value, 0)

  field :feature_column, 1, type: :string, json_name: "featureColumn"

  field :numerical_value, 2,
    type: Google.Protobuf.DoubleValue,
    json_name: "numericalValue",
    oneof: 0

  field :categorical_value, 3,
    type: Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue.CategoricalValue,
    json_name: "categoricalValue",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :centroid_id, 1, type: :int64, json_name: "centroidId"

  field :feature_values, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster.FeatureValue,
    json_name: "featureValues"

  field :count, 3, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.V2.Model.ClusteringMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :davies_bouldin_index, 1,
    type: Google.Protobuf.DoubleValue,
    json_name: "daviesBouldinIndex"

  field :mean_squared_distance, 2,
    type: Google.Protobuf.DoubleValue,
    json_name: "meanSquaredDistance"

  field :clusters, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.ClusteringMetrics.Cluster
end

defmodule Google.Cloud.Bigquery.V2.Model.RankingMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :mean_average_precision, 1,
    type: Google.Protobuf.DoubleValue,
    json_name: "meanAveragePrecision"

  field :mean_squared_error, 2, type: Google.Protobuf.DoubleValue, json_name: "meanSquaredError"

  field :normalized_discounted_cumulative_gain, 3,
    type: Google.Protobuf.DoubleValue,
    json_name: "normalizedDiscountedCumulativeGain"

  field :average_rank, 4, type: Google.Protobuf.DoubleValue, json_name: "averageRank"
end

defmodule Google.Cloud.Bigquery.V2.Model.ArimaForecastingMetrics.ArimaSingleModelForecastingMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :non_seasonal_order, 1,
    type: Google.Cloud.Bigquery.V2.Model.ArimaOrder,
    json_name: "nonSeasonalOrder"

  field :arima_fitting_metrics, 2,
    type: Google.Cloud.Bigquery.V2.Model.ArimaFittingMetrics,
    json_name: "arimaFittingMetrics"

  field :has_drift, 3, type: Google.Protobuf.BoolValue, json_name: "hasDrift"
  field :time_series_id, 4, type: :string, json_name: "timeSeriesId"
  field :time_series_ids, 9, repeated: true, type: :string, json_name: "timeSeriesIds"

  field :seasonal_periods, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.SeasonalPeriod.SeasonalPeriodType,
    json_name: "seasonalPeriods",
    enum: true

  field :has_holiday_effect, 6, type: Google.Protobuf.BoolValue, json_name: "hasHolidayEffect"
  field :has_spikes_and_dips, 7, type: Google.Protobuf.BoolValue, json_name: "hasSpikesAndDips"
  field :has_step_changes, 8, type: Google.Protobuf.BoolValue, json_name: "hasStepChanges"
end

defmodule Google.Cloud.Bigquery.V2.Model.ArimaForecastingMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :arima_single_model_forecasting_metrics, 6,
    repeated: true,
    type:
      Google.Cloud.Bigquery.V2.Model.ArimaForecastingMetrics.ArimaSingleModelForecastingMetrics,
    json_name: "arimaSingleModelForecastingMetrics"
end

defmodule Google.Cloud.Bigquery.V2.Model.DimensionalityReductionMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :total_explained_variance_ratio, 1,
    type: Google.Protobuf.DoubleValue,
    json_name: "totalExplainedVarianceRatio"
end

defmodule Google.Cloud.Bigquery.V2.Model.EvaluationMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:metrics, 0)

  field :regression_metrics, 1,
    type: Google.Cloud.Bigquery.V2.Model.RegressionMetrics,
    json_name: "regressionMetrics",
    oneof: 0

  field :binary_classification_metrics, 2,
    type: Google.Cloud.Bigquery.V2.Model.BinaryClassificationMetrics,
    json_name: "binaryClassificationMetrics",
    oneof: 0

  field :multi_class_classification_metrics, 3,
    type: Google.Cloud.Bigquery.V2.Model.MultiClassClassificationMetrics,
    json_name: "multiClassClassificationMetrics",
    oneof: 0

  field :clustering_metrics, 4,
    type: Google.Cloud.Bigquery.V2.Model.ClusteringMetrics,
    json_name: "clusteringMetrics",
    oneof: 0

  field :ranking_metrics, 5,
    type: Google.Cloud.Bigquery.V2.Model.RankingMetrics,
    json_name: "rankingMetrics",
    oneof: 0

  field :arima_forecasting_metrics, 6,
    type: Google.Cloud.Bigquery.V2.Model.ArimaForecastingMetrics,
    json_name: "arimaForecastingMetrics",
    oneof: 0

  field :dimensionality_reduction_metrics, 7,
    type: Google.Cloud.Bigquery.V2.Model.DimensionalityReductionMetrics,
    json_name: "dimensionalityReductionMetrics",
    oneof: 0
end

defmodule Google.Cloud.Bigquery.V2.Model.DataSplitResult do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :training_table, 1,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "trainingTable"

  field :evaluation_table, 2,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "evaluationTable"

  field :test_table, 3, type: Google.Cloud.Bigquery.V2.TableReference, json_name: "testTable"
end

defmodule Google.Cloud.Bigquery.V2.Model.ArimaOrder do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :p, 1, type: Google.Protobuf.Int64Value
  field :d, 2, type: Google.Protobuf.Int64Value
  field :q, 3, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.V2.Model.ArimaFittingMetrics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :log_likelihood, 1, type: Google.Protobuf.DoubleValue, json_name: "logLikelihood"
  field :aic, 2, type: Google.Protobuf.DoubleValue
  field :variance, 3, type: Google.Protobuf.DoubleValue
end

defmodule Google.Cloud.Bigquery.V2.Model.GlobalExplanation.Explanation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :feature_name, 1, type: :string, json_name: "featureName"
  field :attribution, 2, type: Google.Protobuf.DoubleValue
end

defmodule Google.Cloud.Bigquery.V2.Model.GlobalExplanation do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :explanations, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.GlobalExplanation.Explanation

  field :class_label, 2, type: :string, json_name: "classLabel"
end

defmodule Google.Cloud.Bigquery.V2.Model.CategoryEncodingMethod do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.PcaSolverOptionEnums do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.ModelRegistryOptionEnums do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.TrainingOptions.LabelClassWeightsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :double
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.TrainingOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :max_iterations, 1, type: :int64, json_name: "maxIterations"

  field :loss_type, 2,
    type: Google.Cloud.Bigquery.V2.Model.LossType,
    json_name: "lossType",
    enum: true

  field :learn_rate, 3, type: :double, json_name: "learnRate"
  field :l1_regularization, 4, type: Google.Protobuf.DoubleValue, json_name: "l1Regularization"
  field :l2_regularization, 5, type: Google.Protobuf.DoubleValue, json_name: "l2Regularization"

  field :min_relative_progress, 6,
    type: Google.Protobuf.DoubleValue,
    json_name: "minRelativeProgress"

  field :warm_start, 7, type: Google.Protobuf.BoolValue, json_name: "warmStart"
  field :early_stop, 8, type: Google.Protobuf.BoolValue, json_name: "earlyStop"
  field :input_label_columns, 9, repeated: true, type: :string, json_name: "inputLabelColumns"

  field :data_split_method, 10,
    type: Google.Cloud.Bigquery.V2.Model.DataSplitMethod,
    json_name: "dataSplitMethod",
    enum: true

  field :data_split_eval_fraction, 11, type: :double, json_name: "dataSplitEvalFraction"
  field :data_split_column, 12, type: :string, json_name: "dataSplitColumn"

  field :learn_rate_strategy, 13,
    type: Google.Cloud.Bigquery.V2.Model.LearnRateStrategy,
    json_name: "learnRateStrategy",
    enum: true

  field :initial_learn_rate, 16, type: :double, json_name: "initialLearnRate"

  field :label_class_weights, 17,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.TrainingOptions.LabelClassWeightsEntry,
    json_name: "labelClassWeights",
    map: true

  field :user_column, 18, type: :string, json_name: "userColumn"
  field :item_column, 19, type: :string, json_name: "itemColumn"

  field :distance_type, 20,
    type: Google.Cloud.Bigquery.V2.Model.DistanceType,
    json_name: "distanceType",
    enum: true

  field :num_clusters, 21, type: :int64, json_name: "numClusters"
  field :model_uri, 22, type: :string, json_name: "modelUri"

  field :optimization_strategy, 23,
    type: Google.Cloud.Bigquery.V2.Model.OptimizationStrategy,
    json_name: "optimizationStrategy",
    enum: true

  field :hidden_units, 24, repeated: true, type: :int64, json_name: "hiddenUnits"
  field :batch_size, 25, type: :int64, json_name: "batchSize"
  field :dropout, 26, type: Google.Protobuf.DoubleValue
  field :max_tree_depth, 27, type: :int64, json_name: "maxTreeDepth"
  field :subsample, 28, type: :double
  field :min_split_loss, 29, type: Google.Protobuf.DoubleValue, json_name: "minSplitLoss"

  field :booster_type, 60,
    type: Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.BoosterType,
    json_name: "boosterType",
    enum: true

  field :num_parallel_tree, 61, type: Google.Protobuf.Int64Value, json_name: "numParallelTree"

  field :dart_normalize_type, 62,
    type: Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.DartNormalizeType,
    json_name: "dartNormalizeType",
    enum: true

  field :tree_method, 63,
    type: Google.Cloud.Bigquery.V2.Model.BoostedTreeOptionEnums.TreeMethod,
    json_name: "treeMethod",
    enum: true

  field :min_tree_child_weight, 64,
    type: Google.Protobuf.Int64Value,
    json_name: "minTreeChildWeight"

  field :colsample_bytree, 65, type: Google.Protobuf.DoubleValue, json_name: "colsampleBytree"
  field :colsample_bylevel, 66, type: Google.Protobuf.DoubleValue, json_name: "colsampleBylevel"
  field :colsample_bynode, 67, type: Google.Protobuf.DoubleValue, json_name: "colsampleBynode"
  field :num_factors, 30, type: :int64, json_name: "numFactors"

  field :feedback_type, 31,
    type: Google.Cloud.Bigquery.V2.Model.FeedbackType,
    json_name: "feedbackType",
    enum: true

  field :wals_alpha, 32, type: Google.Protobuf.DoubleValue, json_name: "walsAlpha"

  field :kmeans_initialization_method, 33,
    type: Google.Cloud.Bigquery.V2.Model.KmeansEnums.KmeansInitializationMethod,
    json_name: "kmeansInitializationMethod",
    enum: true

  field :kmeans_initialization_column, 34, type: :string, json_name: "kmeansInitializationColumn"
  field :time_series_timestamp_column, 35, type: :string, json_name: "timeSeriesTimestampColumn"
  field :time_series_data_column, 36, type: :string, json_name: "timeSeriesDataColumn"
  field :auto_arima, 37, type: Google.Protobuf.BoolValue, json_name: "autoArima"

  field :non_seasonal_order, 38,
    type: Google.Cloud.Bigquery.V2.Model.ArimaOrder,
    json_name: "nonSeasonalOrder"

  field :data_frequency, 39,
    type: Google.Cloud.Bigquery.V2.Model.DataFrequency,
    json_name: "dataFrequency",
    enum: true

  field :calculate_p_values, 40, type: Google.Protobuf.BoolValue, json_name: "calculatePValues"
  field :include_drift, 41, type: Google.Protobuf.BoolValue, json_name: "includeDrift"

  field :holiday_region, 42,
    type: Google.Cloud.Bigquery.V2.Model.HolidayRegion,
    json_name: "holidayRegion",
    enum: true

  field :holiday_regions, 71,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.HolidayRegion,
    json_name: "holidayRegions",
    enum: true

  field :time_series_id_column, 43, type: :string, json_name: "timeSeriesIdColumn"

  field :time_series_id_columns, 51,
    repeated: true,
    type: :string,
    json_name: "timeSeriesIdColumns"

  field :forecast_limit_lower_bound, 99, type: :double, json_name: "forecastLimitLowerBound"
  field :forecast_limit_upper_bound, 100, type: :double, json_name: "forecastLimitUpperBound"
  field :horizon, 44, type: :int64
  field :auto_arima_max_order, 46, type: :int64, json_name: "autoArimaMaxOrder"
  field :auto_arima_min_order, 83, type: :int64, json_name: "autoArimaMinOrder"
  field :num_trials, 47, type: :int64, json_name: "numTrials"
  field :max_parallel_trials, 48, type: :int64, json_name: "maxParallelTrials"

  field :hparam_tuning_objectives, 54,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.HparamTuningEnums.HparamTuningObjective,
    json_name: "hparamTuningObjectives",
    enum: true

  field :decompose_time_series, 50,
    type: Google.Protobuf.BoolValue,
    json_name: "decomposeTimeSeries"

  field :clean_spikes_and_dips, 52,
    type: Google.Protobuf.BoolValue,
    json_name: "cleanSpikesAndDips"

  field :adjust_step_changes, 53, type: Google.Protobuf.BoolValue, json_name: "adjustStepChanges"

  field :enable_global_explain, 55,
    type: Google.Protobuf.BoolValue,
    json_name: "enableGlobalExplain"

  field :sampled_shapley_num_paths, 56, type: :int64, json_name: "sampledShapleyNumPaths"

  field :integrated_gradients_num_steps, 57,
    type: :int64,
    json_name: "integratedGradientsNumSteps"

  field :category_encoding_method, 58,
    type: Google.Cloud.Bigquery.V2.Model.CategoryEncodingMethod.EncodingMethod,
    json_name: "categoryEncodingMethod",
    enum: true

  field :tf_version, 70, type: :string, json_name: "tfVersion"

  field :color_space, 72,
    type: Google.Cloud.Bigquery.V2.Model.ColorSpace,
    json_name: "colorSpace",
    enum: true

  field :instance_weight_column, 73, type: :string, json_name: "instanceWeightColumn"
  field :trend_smoothing_window_size, 74, type: :int64, json_name: "trendSmoothingWindowSize"
  field :time_series_length_fraction, 75, type: :double, json_name: "timeSeriesLengthFraction"
  field :min_time_series_length, 76, type: :int64, json_name: "minTimeSeriesLength"
  field :max_time_series_length, 77, type: :int64, json_name: "maxTimeSeriesLength"
  field :xgboost_version, 78, type: :string, json_name: "xgboostVersion"

  field :approx_global_feature_contrib, 84,
    type: Google.Protobuf.BoolValue,
    json_name: "approxGlobalFeatureContrib"

  field :fit_intercept, 85, type: Google.Protobuf.BoolValue, json_name: "fitIntercept"
  field :num_principal_components, 86, type: :int64, json_name: "numPrincipalComponents"
  field :pca_explained_variance_ratio, 87, type: :double, json_name: "pcaExplainedVarianceRatio"
  field :scale_features, 88, type: Google.Protobuf.BoolValue, json_name: "scaleFeatures"

  field :pca_solver, 89,
    type: Google.Cloud.Bigquery.V2.Model.PcaSolverOptionEnums.PcaSolver,
    json_name: "pcaSolver",
    enum: true

  field :auto_class_weights, 90, type: Google.Protobuf.BoolValue, json_name: "autoClassWeights"
  field :activation_fn, 91, type: :string, json_name: "activationFn"
  field :optimizer, 92, type: :string
  field :budget_hours, 93, type: :double, json_name: "budgetHours"

  field :standardize_features, 94,
    type: Google.Protobuf.BoolValue,
    json_name: "standardizeFeatures"

  field :l1_reg_activation, 95, type: :double, json_name: "l1RegActivation"

  field :model_registry, 96,
    type: Google.Cloud.Bigquery.V2.Model.ModelRegistryOptionEnums.ModelRegistry,
    json_name: "modelRegistry",
    enum: true

  field :vertex_ai_model_version_aliases, 97,
    repeated: true,
    type: :string,
    json_name: "vertexAiModelVersionAliases"

  field :dimension_id_columns, 104,
    repeated: true,
    type: :string,
    json_name: "dimensionIdColumns",
    deprecated: false

  field :contribution_metric, 105,
    proto3_optional: true,
    type: :string,
    json_name: "contributionMetric"

  field :is_test_column, 106, proto3_optional: true, type: :string, json_name: "isTestColumn"

  field :min_apriori_support, 107,
    proto3_optional: true,
    type: :double,
    json_name: "minAprioriSupport"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ClusterInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :centroid_id, 1, type: :int64, json_name: "centroidId"
  field :cluster_radius, 2, type: Google.Protobuf.DoubleValue, json_name: "clusterRadius"
  field :cluster_size, 3, type: Google.Protobuf.Int64Value, json_name: "clusterSize"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult.ArimaCoefficients do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :auto_regressive_coefficients, 1,
    repeated: true,
    type: :double,
    json_name: "autoRegressiveCoefficients"

  field :moving_average_coefficients, 2,
    repeated: true,
    type: :double,
    json_name: "movingAverageCoefficients"

  field :intercept_coefficient, 3,
    type: Google.Protobuf.DoubleValue,
    json_name: "interceptCoefficient"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult.ArimaModelInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :non_seasonal_order, 1,
    type: Google.Cloud.Bigquery.V2.Model.ArimaOrder,
    json_name: "nonSeasonalOrder"

  field :arima_coefficients, 2,
    type:
      Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult.ArimaCoefficients,
    json_name: "arimaCoefficients"

  field :arima_fitting_metrics, 3,
    type: Google.Cloud.Bigquery.V2.Model.ArimaFittingMetrics,
    json_name: "arimaFittingMetrics"

  field :has_drift, 4, type: Google.Protobuf.BoolValue, json_name: "hasDrift"
  field :time_series_id, 5, type: :string, json_name: "timeSeriesId"
  field :time_series_ids, 10, repeated: true, type: :string, json_name: "timeSeriesIds"

  field :seasonal_periods, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.SeasonalPeriod.SeasonalPeriodType,
    json_name: "seasonalPeriods",
    enum: true

  field :has_holiday_effect, 7, type: Google.Protobuf.BoolValue, json_name: "hasHolidayEffect"
  field :has_spikes_and_dips, 8, type: Google.Protobuf.BoolValue, json_name: "hasSpikesAndDips"
  field :has_step_changes, 9, type: Google.Protobuf.BoolValue, json_name: "hasStepChanges"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :arima_model_info, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult.ArimaModelInfo,
    json_name: "arimaModelInfo"

  field :seasonal_periods, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.SeasonalPeriod.SeasonalPeriodType,
    json_name: "seasonalPeriods",
    enum: true
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.PrincipalComponentInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :principal_component_id, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "principalComponentId"

  field :explained_variance, 2, type: Google.Protobuf.DoubleValue, json_name: "explainedVariance"

  field :explained_variance_ratio, 3,
    type: Google.Protobuf.DoubleValue,
    json_name: "explainedVarianceRatio"

  field :cumulative_explained_variance_ratio, 4,
    type: Google.Protobuf.DoubleValue,
    json_name: "cumulativeExplainedVarianceRatio"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :index, 1, type: Google.Protobuf.Int32Value
  field :duration_ms, 4, type: Google.Protobuf.Int64Value, json_name: "durationMs"
  field :training_loss, 5, type: Google.Protobuf.DoubleValue, json_name: "trainingLoss"
  field :eval_loss, 6, type: Google.Protobuf.DoubleValue, json_name: "evalLoss"
  field :learn_rate, 7, type: :double, json_name: "learnRate"

  field :cluster_infos, 8,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ClusterInfo,
    json_name: "clusterInfos"

  field :arima_result, 9,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.ArimaResult,
    json_name: "arimaResult"

  field :principal_component_infos, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult.PrincipalComponentInfo,
    json_name: "principalComponentInfos"
end

defmodule Google.Cloud.Bigquery.V2.Model.TrainingRun do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :training_options, 1,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.TrainingOptions,
    json_name: "trainingOptions",
    deprecated: false

  field :start_time, 8, type: Google.Protobuf.Timestamp, json_name: "startTime", deprecated: false

  field :results, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult,
    deprecated: false

  field :evaluation_metrics, 7,
    type: Google.Cloud.Bigquery.V2.Model.EvaluationMetrics,
    json_name: "evaluationMetrics",
    deprecated: false

  field :data_split_result, 9,
    type: Google.Cloud.Bigquery.V2.Model.DataSplitResult,
    json_name: "dataSplitResult",
    deprecated: false

  field :model_level_global_explanation, 11,
    type: Google.Cloud.Bigquery.V2.Model.GlobalExplanation,
    json_name: "modelLevelGlobalExplanation",
    deprecated: false

  field :class_level_global_explanations, 12,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.GlobalExplanation,
    json_name: "classLevelGlobalExplanations",
    deprecated: false

  field :vertex_ai_model_id, 14, type: :string, json_name: "vertexAiModelId"

  field :vertex_ai_model_version, 15,
    type: :string,
    json_name: "vertexAiModelVersion",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace.DoubleRange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :min, 1, type: Google.Protobuf.DoubleValue
  field :max, 2, type: Google.Protobuf.DoubleValue
end

defmodule Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace.DoubleCandidates do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :candidates, 1, repeated: true, type: Google.Protobuf.DoubleValue
end

defmodule Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:search_space, 0)

  field :range, 1,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace.DoubleRange,
    oneof: 0

  field :candidates, 2,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace.DoubleCandidates,
    oneof: 0
end

defmodule Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace.IntRange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :min, 1, type: Google.Protobuf.Int64Value
  field :max, 2, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace.IntCandidates do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :candidates, 1, repeated: true, type: Google.Protobuf.Int64Value
end

defmodule Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:search_space, 0)

  field :range, 1, type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace.IntRange, oneof: 0

  field :candidates, 2,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace.IntCandidates,
    oneof: 0
end

defmodule Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :candidates, 1, repeated: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Model.IntArrayHparamSearchSpace.IntArray do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :elements, 1, repeated: true, type: :int64
end

defmodule Google.Cloud.Bigquery.V2.Model.IntArrayHparamSearchSpace do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :candidates, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.IntArrayHparamSearchSpace.IntArray
end

defmodule Google.Cloud.Bigquery.V2.Model.HparamSearchSpaces do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :learn_rate, 2,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "learnRate"

  field :l1_reg, 3,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "l1Reg"

  field :l2_reg, 4,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "l2Reg"

  field :num_clusters, 26,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "numClusters"

  field :num_factors, 31,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "numFactors"

  field :hidden_units, 34,
    type: Google.Cloud.Bigquery.V2.Model.IntArrayHparamSearchSpace,
    json_name: "hiddenUnits"

  field :batch_size, 37,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "batchSize"

  field :dropout, 38, type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace

  field :max_tree_depth, 41,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "maxTreeDepth"

  field :subsample, 42, type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace

  field :min_split_loss, 43,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "minSplitLoss"

  field :wals_alpha, 49,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "walsAlpha"

  field :booster_type, 56,
    type: Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace,
    json_name: "boosterType"

  field :num_parallel_tree, 57,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "numParallelTree"

  field :dart_normalize_type, 58,
    type: Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace,
    json_name: "dartNormalizeType"

  field :tree_method, 59,
    type: Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace,
    json_name: "treeMethod"

  field :min_tree_child_weight, 60,
    type: Google.Cloud.Bigquery.V2.Model.IntHparamSearchSpace,
    json_name: "minTreeChildWeight"

  field :colsample_bytree, 61,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "colsampleBytree"

  field :colsample_bylevel, 62,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "colsampleBylevel"

  field :colsample_bynode, 63,
    type: Google.Cloud.Bigquery.V2.Model.DoubleHparamSearchSpace,
    json_name: "colsampleBynode"

  field :activation_fn, 67,
    type: Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace,
    json_name: "activationFn"

  field :optimizer, 68, type: Google.Cloud.Bigquery.V2.Model.StringHparamSearchSpace
end

defmodule Google.Cloud.Bigquery.V2.Model.HparamTuningTrial do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :trial_id, 1, type: :int64, json_name: "trialId"
  field :start_time_ms, 2, type: :int64, json_name: "startTimeMs"
  field :end_time_ms, 3, type: :int64, json_name: "endTimeMs"
  field :hparams, 4, type: Google.Cloud.Bigquery.V2.Model.TrainingRun.TrainingOptions

  field :evaluation_metrics, 5,
    type: Google.Cloud.Bigquery.V2.Model.EvaluationMetrics,
    json_name: "evaluationMetrics"

  field :status, 6, type: Google.Cloud.Bigquery.V2.Model.HparamTuningTrial.TrialStatus, enum: true
  field :error_message, 7, type: :string, json_name: "errorMessage"
  field :training_loss, 8, type: Google.Protobuf.DoubleValue, json_name: "trainingLoss"
  field :eval_loss, 9, type: Google.Protobuf.DoubleValue, json_name: "evalLoss"

  field :hparam_tuning_evaluation_metrics, 10,
    type: Google.Cloud.Bigquery.V2.Model.EvaluationMetrics,
    json_name: "hparamTuningEvaluationMetrics"
end

defmodule Google.Cloud.Bigquery.V2.Model.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.Model do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :etag, 1, type: :string, deprecated: false

  field :model_reference, 2,
    type: Google.Cloud.Bigquery.V2.ModelReference,
    json_name: "modelReference",
    deprecated: false

  field :creation_time, 5, type: :int64, json_name: "creationTime", deprecated: false
  field :last_modified_time, 6, type: :int64, json_name: "lastModifiedTime", deprecated: false
  field :description, 12, type: :string, deprecated: false
  field :friendly_name, 14, type: :string, json_name: "friendlyName", deprecated: false
  field :labels, 15, repeated: true, type: Google.Cloud.Bigquery.V2.Model.LabelsEntry, map: true
  field :expiration_time, 16, type: :int64, json_name: "expirationTime", deprecated: false
  field :location, 13, type: :string, deprecated: false

  field :encryption_configuration, 17,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "encryptionConfiguration"

  field :model_type, 7,
    type: Google.Cloud.Bigquery.V2.Model.ModelType,
    json_name: "modelType",
    enum: true,
    deprecated: false

  field :training_runs, 9,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun,
    json_name: "trainingRuns"

  field :feature_columns, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StandardSqlField,
    json_name: "featureColumns",
    deprecated: false

  field :label_columns, 11,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StandardSqlField,
    json_name: "labelColumns",
    deprecated: false

  field :transform_columns, 26,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TransformColumn,
    json_name: "transformColumns",
    deprecated: false

  field :hparam_search_spaces, 18,
    type: Google.Cloud.Bigquery.V2.Model.HparamSearchSpaces,
    json_name: "hparamSearchSpaces",
    deprecated: false

  field :default_trial_id, 21, type: :int64, json_name: "defaultTrialId", deprecated: false

  field :hparam_trials, 20,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.HparamTuningTrial,
    json_name: "hparamTrials",
    deprecated: false

  field :optimal_trial_ids, 22,
    repeated: true,
    type: :int64,
    json_name: "optimalTrialIds",
    deprecated: false

  field :remote_model_info, 25,
    type: Google.Cloud.Bigquery.V2.RemoteModelInfo,
    json_name: "remoteModelInfo",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GetModelRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :model_id, 3, type: :string, json_name: "modelId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PatchModelRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :model_id, 3, type: :string, json_name: "modelId", deprecated: false
  field :model, 4, type: Google.Cloud.Bigquery.V2.Model, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DeleteModelRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :model_id, 3, type: :string, json_name: "modelId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ListModelsRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :project_id, 1, type: :string, json_name: "projectId", deprecated: false
  field :dataset_id, 2, type: :string, json_name: "datasetId", deprecated: false
  field :max_results, 3, type: Google.Protobuf.UInt32Value, json_name: "maxResults"
  field :page_token, 4, type: :string, json_name: "pageToken"
end

defmodule Google.Cloud.Bigquery.V2.ListModelsResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :models, 1, repeated: true, type: Google.Cloud.Bigquery.V2.Model
  field :next_page_token, 2, type: :string, json_name: "nextPageToken"
end

defmodule Google.Cloud.Bigquery.V2.ModelService.Service do
  @moduledoc false

  use GRPC.Service,
    name: "google.cloud.bigquery.v2.ModelService",
    protoc_gen_elixir_version: "0.13.0"

  rpc(:GetModel, Google.Cloud.Bigquery.V2.GetModelRequest, Google.Cloud.Bigquery.V2.Model)

  rpc(
    :ListModels,
    Google.Cloud.Bigquery.V2.ListModelsRequest,
    Google.Cloud.Bigquery.V2.ListModelsResponse
  )

  rpc(:PatchModel, Google.Cloud.Bigquery.V2.PatchModelRequest, Google.Cloud.Bigquery.V2.Model)

  rpc(:DeleteModel, Google.Cloud.Bigquery.V2.DeleteModelRequest, Google.Protobuf.Empty)
end

defmodule Google.Cloud.Bigquery.V2.ModelService.Stub do
  @moduledoc false

  use GRPC.Stub, service: Google.Cloud.Bigquery.V2.ModelService.Service
end
