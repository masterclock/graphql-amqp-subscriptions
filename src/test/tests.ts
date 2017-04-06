import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import { spy, restore } from 'simple-mock';

import * as amqplib from 'amqplib';
import { AmqpPubSub } from '../amqp-pubsub';

chai.use(chaiAsPromised);
const expect = chai.expect;

// -------------- Mocking Redis Client ------------------

let listener;
let consumers = [];

// const assertQueueSpy = spy(() => );
const publishSpy = spy((exchange, routingKey, message) => consumers && consumers.forEach(consumer => {
  console.log('publishSpy: ', routingKey, message, consumer);
  if ((consumer.channel === routingKey) && consumer.cb) {
    console.log('consumer.cb');
    consumer.cb({
      fields: {
        routingKey: routingKey,
      },
      content: message.toString(),
    });
  }
}));

const consumeSpy = spy((queue: string, cb) => {
  console.log('consumeSpy: ', queue, cb);
  consumers.push({
    channel: queue,
    cb: cb,
  });
});

const deleteQueueSpy = spy((channel) => Promise.resolve({}));

const amqplibPackage = amqplib as Object;

const connect = function () {
  return Promise.resolve({
    createChannel: () => ({
      assertExchange: (exchange: string, type: string) => Promise.resolve({}),
      assertQueue: (topic: string) => Promise.resolve({}),
      bindQueue: (queue: string, ex: string, pattern: string) => Promise.resolve({}),
      deleteQueue: deleteQueueSpy,
      publish: publishSpy,
      consume: consumeSpy,
      on: (event, cb) => {
        if (event === 'message') {
          listener = cb;
        }
      },
    }),
  });
};

amqplibPackage['connect'] = connect;

// -------------- Mocking Redis Client ------------------


describe('AmqpPubSub', function () {

  const pubSub = new AmqpPubSub();

  it('can subscribe to specific amqp topic and called when a message is published on it', function (done) {

    let gSubId;
    pubSub.subscribe('Posts', message => {
      console.log(message);
      try {
        expect(message).to.equals('test');
        pubSub.unsubscribe(gSubId);
        done();
      } catch (e) {
        pubSub.unsubscribe(gSubId);
        done(e);
      }

    }).then(subId => {
      expect(subId).to.be.a('number');
      gSubId = subId;
      pubSub.publish('Posts', 'test');
    });
  });

  it('can unsubscribe from specific amqp', function (done) {
    pubSub.subscribe('Posts', () => null).then(subId => {
      pubSub.unsubscribe(subId);
      setTimeout(() => {
        try {
        expect(deleteQueueSpy.callCount).to.equals(1);
        const call = deleteQueueSpy.lastCall;
        expect(call.args).to.have.members(['Posts']);
        done();

      } catch (e) {
        done(e);
      }
      }, 1000);
    });
  });

  it('cleans up correctly the memory when unsubscribing', function (done) {
    Promise.all([
      pubSub.subscribe('Posts', () => null),
      pubSub.subscribe('Posts', () => null),
    ])
      .then(([subId, secondSubId]) => {
        try {
          // This assertion is done against a private member, if you change the internals, you may want to change that
          expect((pubSub as any).subscriptionMap[subId]).not.to.be.an('undefined');
          pubSub.unsubscribe(subId);
          // This assertion is done against a private member, if you change the internals, you may want to change that
          expect((pubSub as any).subscriptionMap[subId]).to.be.an('undefined');
          expect(() => pubSub.unsubscribe(subId)).to.throw(`There is no subscription of id "${subId}"`);
          pubSub.unsubscribe(secondSubId);
          done();

        } catch (e) {
          done(e);
        }
      });
  });

  it('will not unsubscribe from the amqp topic if there is another subscriber on it\'s subscriber list', function (done) {
    const subscriptionPromises = [
      pubSub.subscribe('Posts', () => {
        done('Not supposed to be triggered');
      }),
      pubSub.subscribe('Posts', (msg) => {
        try {
          expect(msg).to.equals('test');
          done();
        } catch (e) {
          done(e);
        }
      }),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);

        pubSub.unsubscribe(subIds[0]);
        expect(deleteQueueSpy.callCount).to.equals(0);

        pubSub.publish('Posts', 'test');
        pubSub.unsubscribe(subIds[1]);
        expect(deleteQueueSpy.callCount).to.equals(1);
      } catch (e) {
        done(e);
      }
    });
  });

  it('will subscribe to redis channel only once', function (done) {
    const onMessage = () => null;
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessage),
      pubSub.subscribe('Posts', onMessage),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);
        expect(consumeSpy.callCount).to.equals(1);

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can have multiple subscribers and all will be called when a message is published to this channel', function (done) {
    const onMessageSpy = spy(() => null);
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessageSpy as Function),
      pubSub.subscribe('Posts', onMessageSpy as Function),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);

        pubSub.publish('Posts', 'test');

        expect(onMessageSpy.callCount).to.equals(2);
        onMessageSpy.calls.forEach(call => {
          expect(call.args).to.have.members(['test']);
        });

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can publish objects as well', function (done) {
    pubSub.subscribe('Posts', message => {
      try {
        expect(message).to.have.property('comment', 'This is amazing');
        done();
      } catch (e) {
        done(e);
      }
    }).then(subId => {
      try {
        pubSub.publish('Posts', { comment: 'This is amazing' });
        pubSub.unsubscribe(subId);
      } catch (e) {
        done(e);
      }
    });
  });

  it('throws if you try to unsubscribe with an unknown id', function () {
    return expect(() => pubSub.unsubscribe(123))
      .to.throw('There is no subscription of id "123"');
  });

  it('can use transform function to convert the trigger name given into more explicit channel name', function (done) {
    const triggerTransform = (trigger, { repoName }) => `${trigger}.${repoName}`;
    const pubsub = new AmqpPubSub({
      triggerTransform,
    });

    const validateMessage = message => {
      try {
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }
    };

    pubsub.subscribe('comments', validateMessage, { repoName: 'graphql-redis-subscriptions' }).then(subId => {
      pubsub.publish('comments.graphql-redis-subscriptions', 'test');
      pubsub.unsubscribe(subId);
    });

  });

  // TODO pattern subs
  beforeEach('Reset consumers', () => {
    consumers = [];
  });

  afterEach('Reset spy count', () => {
    publishSpy.reset();
    consumeSpy.reset();
    deleteQueueSpy.reset();
  });

  after('Restore redis client', () => {
    restore();
  });

});
