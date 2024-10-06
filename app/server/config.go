package main

type Config struct {
	Port             int    `envconfig:"PORT" validate:"required,numeric,min=1,max=65535"`
	DatabaseUrl      string `envconfig:"DATABASE_URL" validate:"omitempty,url"`
	DatabaseHost     string `envconfig:"DATABASE_HOST" validate:"omitempty"`
	DatabasePort     string `envconfig:"DATABASE_PORT" validate:"omitempty"`
	DatabaseUser     string `envconfig:"DATABASE_USER" validate:"omitempty"`
	DatabasePassword string `envconfig:"DATABASE_PASSWORD" validate:"omitempty"`
	DatabaseName     string `envconfig:"DATABASE_NAME" validate:"omitempty"`
}
