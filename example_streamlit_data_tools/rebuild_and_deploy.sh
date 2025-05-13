eval `ssh-agent -s`

ssh-add ssh-add ~/.ssh/id_ed25519

docker buildx build -t streamlit --ssh default .

