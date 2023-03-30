const isValidStateObject = (obj: any) => obj && !Array.isArray(obj) && Object.keys(obj).length;

export class TumblingWindow {
  shardId: string;
  eventSourceARN: string;
  window: {
    start: string;
    end: string;
  } = {
    start: "",
    end: "",
  };
  state: any = {};
  isFinalInvokeForWindow: boolean = false;
  isWindowTerminatedEarly: boolean = false;
  onComplete?: Function;
  constructor({ shardId, eventSourceARN, tumblingWindowInSeconds }: { shardId: string; eventSourceARN: string; tumblingWindowInSeconds: number }) {
    this.shardId = shardId;
    this.eventSourceARN = eventSourceARN;

    const now = new Date();
    this.window.start = now.toISOString();
    now.setSeconds(now.getSeconds() + tumblingWindowInSeconds);
    this.window.end = now.toISOString();
    setTimeout(() => {
      this.isFinalInvokeForWindow = true;
      this.onComplete?.();
    }, tumblingWindowInSeconds * 1000);
  }

  setState(state: any) {
    if (isValidStateObject(state)) {
      if (!this.isFinalInvokeForWindow) {
        this.state = state;
      }

      return this;
    } else {
      throw new Error("Invalide state object");
    }
  }

  getTimeWindowEvent(records: any[]) {
    return {
      Records: records,
      window: this.window,
      state: this.state,
      shardId: this.shardId,
      eventSourceARN: this.eventSourceARN,
      isFinalInvokeForWindow: this.isFinalInvokeForWindow,
      isWindowTerminatedEarly: this.isWindowTerminatedEarly,
    };
  }
}
