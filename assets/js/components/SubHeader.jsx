import Nav from "react-bootstrap/Nav"
import Navbar from "react-bootstrap/Navbar"
const SubHeader = ({paths, actions}) => {
  return (
    <Nav
      style={{
        backgroundColor: "#63ff99",
        top: "66px",
      }}
      className="tw-sticky tw-px-8 tw-py-2 tw-gap-0 tw-flex tw-flex-col"
    >
      <h5 className="tw-text-black tw-text-lg tw-flex tw-justify-start tw-mb-0">
        <span>~/</span>
        {paths.map((path, index) => {
          const hasMore = paths.length !== index + 1
          if (typeof path === "string")
            return <span key={path}>{`${path}${hasMore ? "/" : ""}`}</span>
          if (typeof path === "object") {
            const {to, label} = path
            return (
              <Nav.Link
                key={label}
                href={to}
                className="tw-p-0 tw-text-black"
              >{`${label}${hasMore ? "/" : ""}`}</Nav.Link>
            )
          }
        })}
      </h5>
      <div className="tw-flex tw-flex-row tw-justify-end tw-gap-2">
        {actions.map(({to, html, label}) => (
          <Nav.Link key={to} className="tw-text-black tw-p-1 tw-text-sm" href={to}>
            {html ? <span dangerouslySetInnerHTML={{__html: html}} /> : label}
          </Nav.Link>
        ))}
      </div>
    </Nav>
  )
}
export default SubHeader
