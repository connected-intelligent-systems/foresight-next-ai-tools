import { Kafka } from 'kafkajs'
import env from 'env-var'

const CustomerId = env
  .get('CUSTOMER_ID')
  .default('refit-household-5')
  .required()
  .asString()
const ThingId = env
  .get('THING_ID')
  .default('921c9f30-ef9c-11ee-8e2e-2d4e975467fc')
  .required()
  .asString()
const ClientId = env
  .get('CLIENT_ID')
  .default('test-producer')
  .required()
  .asString()
const KafkaBrokers = env
  .get('KAFKA_BROKERS')
  .default('localhost:9094')
  .required()
  .asString()
const KafkaTopic = env
  .get('KAFKA_TOPIC')
  .default('power-data')
  .required()
  .asString()
const SignalGeneratorFrequency = env
  .get('SIGNAL_GENERATOR_FREQUENCY')
  .default('0.003')
  .required()
  .asFloatPositive()
const SignalGeneratorAmplitude = env
  .get('SIGNAL_GENERATOR_AMPLITUDE')
  .default('2000')
  .required()
  .asFloatPositive()
const SignalGeneratorDutyCycle = env
  .get('SIGNAL_GENERATOR_DUTY_CYCLE')
  .default('1')
  .required()
  .asFloatPositive()
const SignalGenerateSampleRate = env
  .get('SIGNAL_GENERATOR_SAMPLE_RATE')
  .default('0.3333333333')
  .required()
  .asFloatPositive()

/**
 * Calculates the value of a rectangular signal at a given time.
 *
 * @param {number} currentTime - The current time in milliseconds.
 * @param {number} frequency - The frequency of the signal in Hz.
 * @param {number} amplitude - The amplitude of the signal.
 * @param {number} dutyCycle - The duty cycle of the signal as a decimal between 0 and 1.
 * @returns {number} - The value of the rectangular signal at the given time.
 */
const getRectangularSignalValue = (
  currentTime,
  frequency,
  amplitude,
  dutyCycle
) => {
  const period = 1000 / frequency
  const highDuration = period * dutyCycle
  const phaseTime = currentTime % period
  return phaseTime < highDuration ? amplitude : 0
}

/**
 * Delays the execution for the specified number of milliseconds.
 * @param {number} ms - The number of milliseconds to delay the execution.
 * @returns {Promise<void>} - A promise that resolves after the specified delay.
 */
const delay = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function run() {
  const kafka = new Kafka({
    clientId: ClientId,
    brokers: KafkaBrokers.split(',')
  })
  const delayInMs = 1000 / SignalGenerateSampleRate
  const producer = kafka.producer()

  await producer.connect()

  while (true) {
    const signalValue = getRectangularSignalValue(
      Date.now(),
      SignalGeneratorFrequency,
      SignalGeneratorAmplitude,
      SignalGeneratorDutyCycle
    )
    console.log(signalValue)
    const now = Date.now()
    await producer.send({
      topic: KafkaTopic,
      messages: [
        {
          key: ThingId,
          value: JSON.stringify({
            power: signalValue,
            ts: now
          }),
          headers: {
            "tb_msg_md_deviceName": "f9b1b5960dc7156906b418766f335a879297b6c5",
            "tb_msg_md_customer_id": "24965780-249e-11ef-83b1-71cad369311b",
            "tb_msg_md_deviceType": "power",
            "tb_msg_md_ss_nilmDisaggregation": "[{\"url\":\"http://kettle-household-5:8000/predict\",\"duration\":3840000,\"step\":1200000},{\"url\":\"http://dishwasher-household-5:8000/predict\",\"duration\":3840000,\"step\":1200000}]",
            "tb_msg_md_ss_aggregationProperty": "power",
            "tb_msg_md_ts": `${now}`,
            "tb_msg_md_customer_title": "refit-household-7"
          }
        }
      ]
    })
    await delay(delayInMs)
  }
}

run().catch(console.error)
