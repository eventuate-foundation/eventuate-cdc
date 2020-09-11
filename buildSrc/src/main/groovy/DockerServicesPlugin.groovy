import org.gradle.api.Plugin
import org.gradle.api.Project

class DockerServicesPlugin implements Plugin<Project> {
    void apply(Project project) {
        project.ext.composeStartedServices = {
            if (!project.ext.has("composeServices")) {
                return System.env.COMPOSE_SERVICES == null || System.env.COMPOSE_SERVICES == "" ? [] : "$System.env.COMPOSE_SERVICES".split(",")
            } else {
                return "${project.ext.composeServices}".split(",")
            }
        }
    }
}