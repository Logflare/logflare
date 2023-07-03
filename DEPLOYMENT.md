## Deployment

### GRPC Server Setup

To deploy the GRPC server in a new environment you need the following steps for your `<env>`:

- **Cloudflare:** Enable GRPC in the `Network` tab
- **Cloudflare:** Generate a CA Certificate to be used on your Origin server:
  - SSL/TLS -> Origin Server -> Create Certificate -> Create
  - Create .`<env>`.cacert.pem with the content from the certificate field
  - Create .`<env>`.cacert.key with the content from the key field
- **Local:** Generate a self signed certificate for the origin server:
  - `openssl req -newkey rsa:2048 -nodes -days 365000 -keyout .<env>.cert.key -out .<env>.req.pem` and set your domain in the CN option when queried
  - `openssl x509 -req -days 12783 -set_serial 1 -in .<env>.req.pem -out .<env>.cert.pem -CA .<env>.cacert.pem -CAkey .<env>.cacert.key`
  - Store this files to be pushed into the server
- **Google Cloud:** On your `Instance Template`, allow for HTTPS traffic in the Firewall configuration
- **Google Cloud:** On your `Instance Group`, add a new port onto your `Port Mapping` configuration to be `50051` (do check you change all `Instance Groups`)
- **Google Cloud:** Create your Load Balancer
  - Select `HTTP(S) load balancing`
  - Select `From Internet to my VMs or serverless services` and `Global HTTP(S) load balancer`
  - Frontend Configuration
    - Protocol - `HTTPS (includes HTTP/2)`
    - IP Address - Create a new IP Address
    - Set certificate
  - Backend Configuration
    - Create `Backend Service` for each `Instance Group` you want to support
    - Set protocol to `HTTP/2`
    - Select the target `Instance Group` and select the GRPC Port set earlier in the popup
    - Disable Cloud CDN
    - Enable Logging
    - Set Health check
- **Cloudflare:** Set a new DNS route with a sub domain pointing to the generated IP of your GRPC LB
