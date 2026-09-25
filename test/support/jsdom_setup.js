if (!window.document.queryCommandSupported) {
  window.document.queryCommandSupported = () => false;
}

window.MonacoEnvironment = {
  getWorker: () => new window.Worker(),
};
