import * as amqplib from 'amqplib';
import * as uuid from 'uuid';

import { each } from 'async';
import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub';

export interface AmqpOptions {
  url: string;
  options?: {
    frameMax?: number;
    channelMax?: number;
    heartbeat?: number;
    locale?: string;
    [key: string]: any;
  };
}

export interface PubSubAmqpOptions {
  connection?: AmqpOptions;
  exchange?: string;
  triggerTransform?: TriggerTransform;
  connectionListener?: (err: Error) => void;
}

export class AmqpPubSub implements PubSubEngine {

  constructor(options: PubSubAmqpOptions = {}) {
    this.instanceId = uuid.v1();

    this.triggerTransform = options.triggerTransform || (trigger => trigger as string);

    const connection = options.connection || {
      url: 'amqp://10.0.0.2',
      options: {},
    };
    this.amqpConnection = Promise.resolve(amqplib.connect(connection.url, connection.options));
    this.amqpPublisher = this.amqpConnection.then((conn) => Promise.resolve<amqplib.Channel>(conn.createChannel()));
    this.amqpSubscriber = this.amqpConnection.then((conn) => Promise.resolve<amqplib.Channel>(conn.createChannel()));
    this.exchange = options.exchange || 'amq.topic';

    // TODO support for pattern based message
    this.amqpSubscriber.then(sub => {
      // sub.consume(, this.onMessage.bind(this));
      if (options.connectionListener) {
        sub.on('connect', options.connectionListener);
        sub.on('error', options.connectionListener);
      } else {
        sub.on('error', console.error);
      }
    });

    this.amqpPublisher.then(pub => {
      // pub.on('message', this.onMessage.bind(this));
      if (options.connectionListener) {
        pub.on('connect', options.connectionListener);
        pub.on('error', options.connectionListener);
      } else {
        pub.on('error', console.error);
      }
    });

    this.subscriptionMap = {};
    // this.subsRefsMap = {};
    this.currentSubscriptionId = 0;
  }

  public publish(trigger: string, payload: any): boolean | Promise<boolean> {
    // TODO PR graphql-subscriptions to use promises as return value
    return this.amqpPublisher.then(pub => pub.publish(this.exchange, trigger, new Buffer(JSON.stringify(payload))));
  }

  public async subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger, options);
    const id = this.currentSubscriptionId++;
    const sub = await this.amqpSubscriber;
    await sub.assertExchange(this.exchange, 'topic');
    const queueName = `${this.instanceId}-${id}-${triggerName}`;
    this.subscriptionMap[id] = queueName;
    await sub.assertQueue(queueName, {
      // exclusive: true,
      autoDelete: true,
    });
    await sub.bindQueue(queueName, this.exchange, triggerName);
    sub.consume(queueName, (msg) => {
      if (!msg) {
        return;
      }
      console.log('consume: ', queueName, msg);
      if (!(this.subscriptionMap[id] === queueName)) {
        return;
      }
      let parsedMessage;
      try {
        parsedMessage = JSON.parse(msg.content.toString());
      } catch (e) {
        parsedMessage = msg;
      }
      onMessage(parsedMessage);
    }, {
      consumerTag: queueName,
      noAck: true,
    });
    return id;
  }

  public unsubscribe(subId: number): Promise<any> {
    const consumerTag = this.subscriptionMap[subId];
    if (!consumerTag) {
      return Promise.resolve(true);
    }
    delete this.subscriptionMap[subId];
    this.amqpSubscriber.then(sub => {
      sub.cancel(consumerTag);
    });
    return Promise.resolve(true);
  }

  // private onMessage(message: amqplib.Message) {
  //   console.log('onMessage: ', message);
  //   if (!message) {
  //     return;
  //   }
  //   const channel = message.fields.routingKey;
  //   const subscribers = this.subsRefsMap[channel];

  //   // Don't work for nothing..
  //   if (!subscribers || !subscribers.length) {
  //     return;
  //   }

  //   let parsedMessage;
  //   try {
  //     parsedMessage = JSON.parse(message.content.toString());
  //   } catch (e) {
  //     parsedMessage = message;
  //   }

  //   each(subscribers, (subId, cb) => {
  //     // TODO Support pattern based subscriptions
  //     console.log('subscribers: ', subscribers);
  //     const [triggerName, listener] = this.subscriptionMap[subId];
  //     listener(parsedMessage);
  //     cb();
  //   });
  // }

  private instanceId: string;

  private triggerTransform: TriggerTransform;
  private amqpConnection: Promise<amqplib.Connection>;
  private amqpSubscriber: Promise<amqplib.Channel>;
  private amqpPublisher: Promise<amqplib.Channel>;
  private exchange: string;

  private subscriptionMap: { [subId: number]: string };
  // private subsRefsMap: { [trigger: string]: Array<number> };
  private currentSubscriptionId: number;
}

export type Path = Array<string | number>;
export type Trigger = string | Path;
export type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;
