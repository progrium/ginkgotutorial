def service():
    import sys, logging, os
    from ginkgo.core import Service
    from gtutorial.cluster import ClusterCoordinator

    logger = logging.getLogger(__name__)

    class ClusterTest(Service):
        def __init__(self, identity, leader=None):
            self.cluster = ClusterCoordinator(identity, leader)

            self.add_service(self.cluster)

        def do_start(self):
            def show_cluster(add=None, remove=None):
                logger.info(self.cluster.set)
            self.cluster.set.attach(show_cluster)
            logger.info(self.cluster.set)
            self.spawn(self.wait_for_promotion)

        def wait_for_promotion(self):
            self.cluster.wait_for_promotion()
            logger.info("Promoted to leader")

    return ClusterTest(
        os.environ.get("IDENTITY", "127.0.0.1"),
        os.environ.get("LEADER"))
