use super::*;

impl Subscriptions {
    /// Set up local delivery without changing the HTTP callback acceptance policy.
    pub(crate) fn subscribe_for_delivery_test(
        &self,
        service: Service,
        callbacks: Vec<Url>,
    ) -> Uuid {
        let (sid, _) = self
            .apply(
                service,
                Ipv4Addr::LOCALHOST,
                SubscriptionRequest::Subscribe {
                    callbacks,
                    lease: SUBSCRIPTION_LEASE,
                },
            )
            .unwrap();

        self.wake.notify_one();

        sid
    }
}
