//! Lane kinds that [`bevy_replicon`]'s [`Channel`] has no word for.
//!
//! Replicon 0.44's [`Channel`] is a closed enum of three kinds, and only
//! replicon creates channels: one per registered message or event, its id its
//! place in [`RepliconChannels`]. So a lane kind of ours, such as
//! [`LaneKind::Redundant`], is attached to the channel id replicon gave a
//! message ([`RemoteMessageRegistry`] tells us which), in [`ChannelLanes`],
//! and the transport is built with it in place of [`convert::to_lane_kind`].
//!
//! Both peers must attach the same lanes to the same channels, as they must
//! register the same messages: do it next to the registration, in the plugin
//! both add. The lane is wire-compatible with the others, but a peer that
//! receives it on a reliable lane waits forever for the messages that expired,
//! so the attachment goes into replicon's protocol hash: peers that disagree
//! are refused at connect, as for any other protocol difference.
//!
//! [`convert::to_lane_kind`]: crate::convert::to_lane_kind

use {
    crate::convert,
    aeronet_transport::lane::LaneKind,
    bevy_app::prelude::*,
    bevy_ecs::prelude::*,
    bevy_platform::collections::HashMap,
    bevy_replicon::{
        prelude::*,
        shared::{message::registry::RemoteMessageRegistry, protocol::ProtocolHasher},
    },
    core::{any::type_name, time::Duration},
};

/// Lane kinds that replace [`convert::to_lane_kind`] for some channels, by
/// channel id.
///
/// [`convert::to_lane_kind`]: crate::convert::to_lane_kind
#[derive(Debug, Clone, Default, Resource)]
pub struct ChannelLanes {
    /// Server -> client channels ([`RepliconChannels::server_channels`]).
    pub server: HashMap<usize, LaneKind>,
    /// Client -> server channels ([`RepliconChannels::client_channels`]).
    pub client: HashMap<usize, LaneKind>,
}

impl ChannelLanes {
    /// The lane kind of each of `channels`, by channel id: the attached one
    /// where `overrides` has one, else [`convert::to_lane_kind`].
    ///
    /// [`convert::to_lane_kind`]: crate::convert::to_lane_kind
    pub fn lanes<'a>(
        channels: &'a [Channel],
        overrides: &'a HashMap<usize, LaneKind>,
    ) -> impl Iterator<Item = LaneKind> + 'a {
        channels.iter().enumerate().map(|(id, channel)| {
            overrides
                .get(&id)
                .copied()
                .unwrap_or_else(|| convert::to_lane_kind(*channel))
        })
    }
}

/// Sends replicon events on a [`LaneKind::Redundant`] lane.
pub trait RedundantEventAppExt {
    /// Sends server event `E` on a [`LaneKind::Redundant`] lane: in every
    /// packet until acknowledged, given up `expiry` after it was sent,
    /// delivered at most once and in no particular order.
    ///
    /// `E` must already be registered (`add_server_event`); register it with
    /// [`Channel::Unordered`], which is what other backends give it. Call it
    /// in the plugin both peers add.
    ///
    /// Replicon still queues a server event on the client until the entity
    /// changes of its tick have arrived on its reliable updates channel, so
    /// after a burst of loss the event would wait for that channel's resend
    /// and the lane would gain nothing. An event that holds no entities can
    /// skip the wait: mark it independent too (`make_event_independent`).
    ///
    /// # Panics
    ///
    /// Panics if `E` is not registered as a server event yet.
    fn make_server_event_redundant<E: Event>(&mut self, expiry: Duration) -> &mut Self;
}

impl RedundantEventAppExt for App {
    fn make_server_event_redundant<E: Event>(&mut self, expiry: Duration) -> &mut Self {
        let channel = self
            .world()
            .get_resource::<RemoteMessageRegistry>()
            .and_then(RemoteMessageRegistry::server_event_channel::<E>)
            .unwrap_or_else(|| {
                panic!(
                    "{} must be registered with `add_server_event` before it is made redundant",
                    type_name::<E>()
                )
            });
        self.world_mut()
            .get_resource_or_init::<ChannelLanes>()
            .server
            .insert(channel, LaneKind::Redundant { expiry });
        // the lane, not its expiry: only the sender reads that
        self.world_mut()
            .resource_mut::<ProtocolHasher>()
            .add_custom(("aeronet redundant server channel", channel));
        self
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bevy::{prelude::*, state::app::StatesPlugin},
        serde::{Deserialize, Serialize},
    };

    #[derive(Event, Serialize, Deserialize)]
    struct Blast;

    #[derive(Event, Serialize, Deserialize)]
    struct Chat;

    #[test]
    fn a_redundant_event_gets_its_lane_and_nothing_else_changes() {
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins))
            .add_server_event::<Chat>(Channel::Ordered)
            .add_server_event::<Blast>(Channel::Unordered)
            .make_server_event_redundant::<Blast>(Duration::from_millis(750));
        let blast = app
            .world()
            .resource::<RemoteMessageRegistry>()
            .server_event_channel::<Blast>()
            .unwrap();
        let channels = app.world().resource::<RepliconChannels>();
        let overrides = app.world().resource::<ChannelLanes>();
        let lanes =
            ChannelLanes::lanes(channels.server_channels(), &overrides.server).collect::<Vec<_>>();
        let plain = channels
            .server_channels()
            .iter()
            .map(|channel| convert::to_lane_kind(*channel));
        for (id, (lane, plain)) in lanes.iter().zip(plain).enumerate() {
            if id == blast {
                assert_eq!(
                    *lane,
                    LaneKind::Redundant {
                        expiry: Duration::from_millis(750)
                    }
                );
            } else {
                assert_eq!(*lane, plain);
            }
        }
        assert!(overrides.client.is_empty());
    }

    fn protocol(redundant: bool) -> bevy_replicon::shared::protocol::ProtocolHash {
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins))
            .add_server_event::<Blast>(Channel::Unordered);
        if redundant {
            app.make_server_event_redundant::<Blast>(Duration::from_millis(750));
        }
        app.finish();
        *app.world()
            .resource::<bevy_replicon::shared::protocol::ProtocolHash>()
    }

    /// A peer that sends a redundant lane can't connect to one that would take
    /// it for a reliable one.
    #[test]
    fn the_lane_is_in_the_protocol() {
        assert_ne!(protocol(true), protocol(false));
        assert_eq!(protocol(true), protocol(true));
    }

    #[test]
    #[should_panic = "must be registered"]
    fn an_unregistered_event_panics() {
        let mut app = App::new();
        app.add_plugins((MinimalPlugins, StatesPlugin, RepliconPlugins))
            .make_server_event_redundant::<Blast>(Duration::from_millis(750));
    }
}
