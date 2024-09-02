
import DataGrid from 'react-data-grid';

function DataTable({columns, rows} = props) {
  console.log(props)
  return <DataGrid columns={columns}
  rowGetter={i => rows[i]}
  rowsCount={3}
  minHeight={150} 
   />;
}

export default DataTable