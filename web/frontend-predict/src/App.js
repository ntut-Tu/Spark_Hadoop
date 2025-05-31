import logo from './logo.svg';
import './App.css';
import BatchPredictForm from "./components/BatchPredictForm";
import SinglePredictForm from "./components/SinglePredictForm";

function App() {
  return (
    <div style={{ padding: "20px" }}>
      <h1>🎯 預測系統前端</h1>
      <SinglePredictForm />
      <hr />
      <BatchPredictForm />
    </div>
  );
}

export default App;
