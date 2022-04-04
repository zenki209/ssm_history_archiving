resource "aws_lambda_function" "test_lambda" {
  filename      = "lambda_function_payload.zip"
  function_name = "lambda_function_achive_history_arm"
  role          = aws_iam_role.iam_for_lambda.arn

  runtime = "nodejs12.x"

  environment {
    variables = {
      foo = "bar"
    }
  }
}