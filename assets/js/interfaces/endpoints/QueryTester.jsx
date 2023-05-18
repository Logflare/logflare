import {useEffect, useState} from "react"
import Button from "react-bootstrap/Button"
import InputGroup from "react-bootstrap/InputGroup"
import Form from "react-bootstrap/Form"
import JsonResults from "./JsonResults.jsx"

const QueryTester = ({
  declaredParams,
  onRunQuery,
  queryResultRows,
  variant = "vertical",
  className = "",
  showQuickRunPrompt = false,
  parameters={},
  onParametersChange
}) => {
  const [params, setParams] = useState(parameters)

  const handleRun = () => onRunQuery(params)
  const resetParams = () => setParams({})
  const handleParamChange = async (e) => {
    const name = e.target.name
    const value = e.target.value
    setParams((prev) => ({...prev, [name]: value}))
  }

  const handleSubmit = (e) => {
    e.preventDefault()
    handleRun(params)
  }

  useEffect(()=>{
    if (onParametersChange) onParametersChange(params)
  }, [params])

  return (
    <div
      className={[
        "tw-flex tw-gap-4",
        variant === "vertical" ? "tw-flex-col" : "",
        variant === "horizontal" ? "tw-flex-row" : "",
        className,
      ].join(" ")}
    >
      <Form className="tw-flex tw-flex-col tw-gap-2 tw-min-w-max" onSubmit={handleSubmit}>
        {declaredParams.length > 0 && <h5>Query Parameters</h5>}

        {declaredParams.map((paramKey) => (
          <Form.Group
            key={paramKey}
            className="tw-w-72"
            controlId={`params-${paramKey}`}
          >
            <InputGroup>
              <InputGroup.Text className="tw-font-mono">
                {paramKey}
              </InputGroup.Text>
              <Form.Control
                type="text"
                name={paramKey}
                onChange={handleParamChange}
                value={params[paramKey] || ""}
              />
            </InputGroup>
          </Form.Group>
        ))}
        <div>
          {declaredParams.length > 0 && (
            <Button variant="secondary" onClick={resetParams}>
              Reset
            </Button>
          )}
          <Button variant="primary" onClick={handleRun}>
            Run Query
          </Button>
        </div>
      </Form>

      <div className="tw-w-full tw-flex tw-flex-col">
        <h5>Query Results</h5>

        {queryResultRows && (
          <JsonResults className="mt-2 tw-flex-grow" data={queryResultRows} />
        )}
        {!queryResultRows && (
          <div>
            <p className="tw-whitespace-pre-wrap">Run the query to get test results.

              {showQuickRunPrompt  && "\nYou can also use CTRL + Enter to trigger a test run."}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

export default QueryTester
