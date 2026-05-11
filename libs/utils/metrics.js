const formatLabel = labels => {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) return '';
  return '{' + keys.map(k => `${k}="${labels[k]}"`).join(',') + '}';
};

const getValuesMap = collector => {
  const result = {};
  for (const [key, value] of collector._values) {
    const labelVals = key.split('\0');
    const labels = {};
    collector.labelNames.forEach((k, i) => {
      if (labelVals[i] !== '') labels[k] = labelVals[i];
    });
    const labelKey = labels.topic || '_all';
    result[labelKey] = value;
  }
  return result;
};

class Counter {
  constructor({ name, help, labelNames, maxCardinality = 1000 }) {
    this.name = name;
    this.help = help;
    this.labelNames = labelNames || [];
    this.maxCardinality = maxCardinality;
    this._values = new Map();
  }

  _key(labels) {
    return this.labelNames.map(k => labels[k] || '').join('\0');
  }

  inc(labels = {}) {
    const key = this._key(labels);
    if (this._values.has(key)) {
      this._values.set(key, this._values.get(key) + 1);
    } else if (this._values.size < this.maxCardinality) {
      this._values.set(key, 1);
    }
  }

  format() {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} counter`];
    for (const [key, value] of this._values) {
      const labelVals = key.split('\0');
      const labels = {};
      this.labelNames.forEach((k, i) => {
        if (labelVals[i] !== '') labels[k] = labelVals[i];
      });
      lines.push(`${this.name}${formatLabel(labels)} ${value}`);
    }
    return lines.join('\n');
  }
}

class Gauge {
  constructor({ name, help, labelNames, maxCardinality = 1000 }) {
    this.name = name;
    this.help = help;
    this.labelNames = labelNames || [];
    this.maxCardinality = maxCardinality;
    this._values = new Map();
  }

  _key(labels) {
    return this.labelNames.map(k => labels[k] || '').join('\0');
  }

  set(labels, value) {
    if (typeof labels === 'number') {
      value = labels;
      labels = {};
    }
    const key = this._key(labels);
    if (this._values.has(key) || this._values.size < this.maxCardinality) {
      this._values.set(key, value);
    }
  }

  format() {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} gauge`];
    for (const [key, value] of this._values) {
      const labelVals = key.split('\0');
      const labels = {};
      this.labelNames.forEach((k, i) => {
        if (labelVals[i] !== '') labels[k] = labelVals[i];
      });
      lines.push(`${this.name}${formatLabel(labels)} ${value}`);
    }
    return lines.join('\n');
  }
}

class Sampler {
  constructor(registry, { interval = 10000, maxSamples = 360 } = {}) {
    this._registry = registry;
    this._interval = interval;
    this._maxSamples = maxSamples;
    this._samples = [];
    this._timer = null;
  }

  start() {
    if (this._timer) return;
    this._timer = setInterval(() => this._takeSample(), this._interval);
    this._timer.unref();
    this._takeSample();
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  _takeSample() {
    const snapshot = { timestamp: Date.now() };
    for (const collector of this._registry._collectors) {
      snapshot[collector.name] = getValuesMap(collector);
    }
    this._samples.push(snapshot);
    if (this._samples.length > this._maxSamples) {
      this._samples.shift();
    }
  }

  getSamples() {
    return this._samples;
  }

  getRate(metricName, windowMs = 300000) {
    const now = Date.now();
    const cutoff = now - windowMs;
    const samples = this._samples.filter(s => s.timestamp >= cutoff);
    if (samples.length < 2) {
      const current = this._samples.length > 0 ? this._samples[this._samples.length - 1] : null;
      if (!current || !current[metricName]) return {};
      const result = {};
      for (const [topic, val] of Object.entries(current[metricName])) {
        result[topic] = 0;
      }
      return result;
    }

    const oldest = samples[0];
    const latest = samples[samples.length - 1];
    const elapsedSec = (latest.timestamp - oldest.timestamp) / 1000;
    if (elapsedSec <= 0) return {};

    const result = {};
    const latestValues = latest[metricName] || {};
    const oldestValues = oldest[metricName] || {};

    const allTopics = new Set([...Object.keys(latestValues), ...Object.keys(oldestValues)]);
    for (const topic of allTopics) {
      const diff = (latestValues[topic] || 0) - (oldestValues[topic] || 0);
      result[topic] = diff / elapsedSec;
    }
    return result;
  }

  getTimeSeries(metricName, { windowMs, stepMs = 60000, rate = false } = {}) {
    const now = Date.now();
    const start = windowMs ? now - windowMs : this._samples.length > 0 ? this._samples[0].timestamp : now;
    const samples = this._samples.filter(s => s.timestamp >= start);
    if (samples.length === 0) return [];

    if (rate) {
      return this._buildRateTimeSeries(metricName, samples, stepMs, windowMs || 300000);
    }

    const buckets = this._bucketSamples(samples, stepMs);
    return buckets.map(bucket => {
      const point = { timestamp: bucket.timestamp };
      const values = bucket.sample[metricName] || {};
      for (const [topic, val] of Object.entries(values)) {
        point[topic] = val;
      }
      return point;
    });
  }

  _buildRateTimeSeries(metricName, samples, stepMs, windowMs) {
    const buckets = this._bucketSamples(samples, stepMs);
    const windowSamples = this._samples.filter(s => s.timestamp >= Date.now() - windowMs);

    return buckets.map(bucket => {
      const point = { timestamp: bucket.timestamp };
      const currentValues = bucket.sample[metricName] || {};

      const olderSample = windowSamples.find(s => s.timestamp <= bucket.timestamp - windowMs + stepMs);
      if (!olderSample) {
        for (const topic of Object.keys(currentValues)) {
          point[topic] = 0;
        }
        return point;
      }

      const elapsedSec = (bucket.timestamp - olderSample.timestamp) / 1000;
      if (elapsedSec <= 0) {
        for (const topic of Object.keys(currentValues)) {
          point[topic] = 0;
        }
        return point;
      }

      const olderValues = olderSample[metricName] || {};
      const allTopics = new Set([...Object.keys(currentValues), ...Object.keys(olderValues)]);
      for (const topic of allTopics) {
        point[topic] = ((currentValues[topic] || 0) - (olderValues[topic] || 0)) / elapsedSec;
      }
      return point;
    });
  }

  _bucketSamples(samples, stepMs) {
    if (samples.length === 0) return [];
    const buckets = [];
    const startTime = samples[0].timestamp;
    const lastTime = samples[samples.length - 1].timestamp;
    for (let t = startTime; t <= lastTime; t += stepMs) {
      const idx = this._binarySearchClosest(samples, t);
      buckets.push({ timestamp: t, sample: samples[idx] });
    }
    return buckets;
  }

  _binarySearchClosest(samples, target) {
    let lo = 0;
    let hi = samples.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (samples[mid].timestamp < target) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    if (lo === 0) return 0;
    const dLo = Math.abs(samples[lo].timestamp - target);
    const dPrev = Math.abs(samples[lo - 1].timestamp - target);
    return dPrev <= dLo ? lo - 1 : lo;
  }
}

class Registry {
  constructor() {
    this._collectors = [];
  }

  register(collector) {
    this._collectors.push(collector);
  }

  metrics() {
    return this._collectors.map(c => c.format()).join('\n');
  }
}

const registry = new Registry();

const metrics = {
  queueDepth: new Gauge({
    name: 'mq_queue_depth',
    help: 'Current queue depth',
    labelNames: ['topic']
  }),
  consumedTotal: new Counter({
    name: 'mq_consumed_total',
    help: 'Total consumed messages',
    labelNames: ['topic']
  }),
  failedTotal: new Counter({
    name: 'mq_failed_total',
    help: 'Total failed messages',
    labelNames: ['topic']
  }),
  dlqTotal: new Counter({
    name: 'mq_dlq_total',
    help: 'Total DLQ messages',
    labelNames: ['topic']
  })
};

Object.values(metrics).forEach(m => registry.register(m));

const sampler = new Sampler(registry);

module.exports = { registry, metrics, sampler, getValuesMap };
