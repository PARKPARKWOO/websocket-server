package main

import "testing"

func clearResellWebSocketEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("RESELL_WEBSOCKET_ENABLED", "")
	t.Setenv("RESELL_APPLICATION_ID", "")
	t.Setenv("WEBSOCKET_ALLOWED_ORIGINS", "")
	t.Setenv("RESELL_REDIS_CHANNEL_PREFIX", "")
}

func TestLoadResellWebSocketConfigDefaultsToDisabled(t *testing.T) {
	clearResellWebSocketEnvironment(t)

	config, err := loadResellWebSocketConfig()
	if err != nil {
		t.Fatalf("loadResellWebSocketConfig() error = %v", err)
	}
	if config != nil {
		t.Fatalf("loadResellWebSocketConfig() = %#v, want nil", config)
	}
}

func TestLoadResellWebSocketConfigExplicitFalseIgnoresResellSettings(t *testing.T) {
	clearResellWebSocketEnvironment(t)
	t.Setenv("RESELL_WEBSOCKET_ENABLED", "false")
	t.Setenv("RESELL_APPLICATION_ID", " invalid padded value ")
	t.Setenv("WEBSOCKET_ALLOWED_ORIGINS", "https://*.example.com")
	t.Setenv("RESELL_REDIS_CHANNEL_PREFIX", "*")

	config, err := loadResellWebSocketConfig()
	if err != nil {
		t.Fatalf("disabled config error = %v", err)
	}
	if config != nil {
		t.Fatalf("disabled config = %#v, want nil", config)
	}
}

func TestLoadResellWebSocketConfigRejectsInvalidFeatureFlag(t *testing.T) {
	clearResellWebSocketEnvironment(t)
	t.Setenv("RESELL_WEBSOCKET_ENABLED", "enabled")

	if _, err := loadResellWebSocketConfig(); err == nil {
		t.Fatal("invalid RESELL_WEBSOCKET_ENABLED was accepted")
	}
}

func TestLoadResellWebSocketConfigFailsClosedWhenEnabledWithoutSecurityConfig(t *testing.T) {
	t.Run("missing application id", func(t *testing.T) {
		clearResellWebSocketEnvironment(t)
		t.Setenv("RESELL_WEBSOCKET_ENABLED", "true")
		t.Setenv("WEBSOCKET_ALLOWED_ORIGINS", "https://resell.platformholder.site")

		if _, err := loadResellWebSocketConfig(); err == nil {
			t.Fatal("enabled config without RESELL_APPLICATION_ID was accepted")
		}
	})

	t.Run("missing origins", func(t *testing.T) {
		clearResellWebSocketEnvironment(t)
		t.Setenv("RESELL_WEBSOCKET_ENABLED", "true")
		t.Setenv("RESELL_APPLICATION_ID", "resell-application-id")

		if _, err := loadResellWebSocketConfig(); err == nil {
			t.Fatal("enabled config without WEBSOCKET_ALLOWED_ORIGINS was accepted")
		}
	})
}

func TestLoadResellWebSocketConfigBuildsEnabledConfig(t *testing.T) {
	clearResellWebSocketEnvironment(t)
	t.Setenv("RESELL_WEBSOCKET_ENABLED", "true")
	t.Setenv("RESELL_APPLICATION_ID", "resell-application-id")
	t.Setenv("WEBSOCKET_ALLOWED_ORIGINS", "https://resell.platformholder.site")

	config, err := loadResellWebSocketConfig()
	if err != nil {
		t.Fatalf("enabled config error = %v", err)
	}
	if config == nil {
		t.Fatal("enabled config is nil")
	}
	if config.ApplicationID != "resell-application-id" {
		t.Fatalf("ApplicationID = %q", config.ApplicationID)
	}
	if config.RedisChannelPattern != "dev-resell:*" {
		t.Fatalf("RedisChannelPattern = %q", config.RedisChannelPattern)
	}
}
