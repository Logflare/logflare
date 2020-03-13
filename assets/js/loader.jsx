import { DotLoader } from "react-spinners"

const brandGreen = "#5eeb8f"

const Loader = () => {
  return (
    <div
      style={{
        height: 400,
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <DotLoader size={70} color={brandGreen} />
    </div>
  )
}

export default Loader
