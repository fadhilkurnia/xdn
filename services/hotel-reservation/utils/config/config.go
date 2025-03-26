package config

import (
	"fmt"
	//"os"
    //"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
    MongoURI            string  `mapstructure:"MONGO_URI"`
    Port                int     `mapstructure:"PORT"`
    DbAttractions       string  `mapstructure:"DB_ATTRACTIONS"`
    DbGeo               string  `mapstructure:"DB_GEO"`
    DbProfile           string  `mapstructure:"DB_PROFILE"`
    DbRate              string  `mapstructure:"DB_RATE"`
    DbRecommendation    string  `mapstructure:"DB_RECOMMENDATION"`
    DbReservation       string  `mapstructure:"DB_RESERVATION"`
    DbReview            string  `mapstructure:"DB_REVIEW"`
    DbUser              string  `mapstructure:"DB_USER"`
}

func LoadConfig() (Config, error) {
    // Check if the code runs from the root folder
    /*
    dir, _ := os.Getwd()
    dirName := filepath.Base(dir)
    if dirName != "hotelReservation" {
        return Config{}, fmt.Errorf("The code must run on the 'hotelReservation' root folder.")
    }
    */

    // Loads config.env
    viper.AddConfigPath(".")
    viper.SetConfigName("config")
    viper.SetConfigType("env")
    viper.AutomaticEnv()
    err := viper.ReadInConfig()

    if err != nil {
        _, fileNotFound := err.(viper.ConfigFileNotFoundError)
        if fileNotFound {
            return Config{}, fmt.Errorf("config.env file not found.")
        }

        return Config{}, fmt.Errorf("Failed to read config file: %w", err)
    }

    // Unmarshal then returns config{}
    var config Config
    err = viper.Unmarshal(&config)
    if err != nil {
        return Config{}, fmt.Errorf("Failed to unmarshal config file: %w", err)
    }

    return config, nil
}
