def service():
    import sys, logging, os
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)

    from gtutorial.cluster import ClusterCoordinator
    from gservice.core import Service
    from gtutorial.util import ObservableSet

    logger = logging.getLogger(__name__)

    class ClusterTest(Service):
        def __init__(self, identity, leader=None):
            self.cluster = ObservableSet()
            self.coordinator = ClusterCoordinator(identity, leader,
                self.cluster)

            self.add_service(self.coordinator)

        def do_start(self):
            def show_cluster(add=None, remove=None):
                logger.info(self.cluster)
            self.cluster.attach(show_cluster)
            logger.info(self.cluster)
            self.spawn(self.wait_for_promotion)

        def wait_for_promotion(self):
            self.coordinator.wait_for_promotion()
            logger.info("Promoted to leader")

    return ClusterTest(
        os.environ.get("IDENTITY", "127.0.0.1"),
        os.environ.get("LEADER"))
