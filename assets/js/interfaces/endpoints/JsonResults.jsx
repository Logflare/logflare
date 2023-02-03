const JsonResults = ({data, className = ""}) => (
  <div className={[
    className,
    "tw-rounded tw-p-2"
  ].join(" ")}>
    <pre>{JSON.stringify(data, false, 2)}</pre>
  </div>
)

export default JsonResults
