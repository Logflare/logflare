import Nav from "react-bootstrap/Nav"
import Navbar from "react-bootstrap/Navbar"
const SubHeader = ({paths}) => {
  return (
      <Nav
        style={{
          backgroundColor: "#63ff99",
          top: "66px",
        }}
        className="tw-sticky tw-px-8 tw-py-4"
      >
        <h5 className="tw-text-black tw-text-lg tw-flex tw-justify-start">
          <span>~/</span>
          {paths.map((path, index) => {
            const hasMore = paths.length !== index + 1
            if (typeof path === "string") return <span key={path}>{`${path}${hasMore ? "/" : ""}`}</span>
            if (typeof path === "object") {
              const {to, label} = path
              return (
                <Nav.Link  key={label} href={to} className="tw-p-0 tw-text-black">{`${label}${hasMore ? "/" : ""}`}</Nav.Link>
              )
            }
          })}
        </h5>
      </Nav>
  )
}
export default SubHeader
