const JsonResults = ({data, className}) => (
  <div className={className}>
    <pre>{JSON.stringify(data, false, 2)}</pre>
  </div>
)

export default JsonResults
