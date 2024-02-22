terraform {
        required_providers {
                docker = {
                        source = "kreuzwerker/docker"
                        version = "2.11.0"
                }
        }

}

resource "docker_network" "network" {
        name = "hackerrank_network"
        driver = "bridge"
}

resource "docker_volume" "nginx" {
        name = "nginx"
        driver = "local"
        driver_opts = {
                type = "none"
                device = "/etc/nginx"
                o = "bind"
}
}

resource "docker_container" "nginx" {
        name = "hackerrank_nginx"
        image = "nginx:1.17"
        hostname = "hackerrank_nginx"
        networks_advanced {
                name = docker_network.network.name
        }
        volumes {
                volume_name = docker_volume.nginx.name
                container_path = "/etc/nginx/conf.d"
                read_only = true
        }
        ports {
                internal = 80
                external = 80
        }
}